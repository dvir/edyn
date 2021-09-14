#include "edyn/parallel/island_worker.hpp"
#include "edyn/collision/contact_manifold.hpp"
#include "edyn/collision/contact_point.hpp"
#include "edyn/comp/orientation.hpp"
#include "edyn/comp/tag.hpp"
#include "edyn/config/config.h"
#include "edyn/math/quaternion.hpp"
#include "edyn/math/vector3.hpp"
#include "edyn/parallel/job.hpp"
#include "edyn/comp/island.hpp"
#include "edyn/parallel/message_dispatcher.hpp"
#include "edyn/shapes/compound_shape.hpp"
#include "edyn/shapes/convex_mesh.hpp"
#include "edyn/shapes/polyhedron_shape.hpp"
#include "edyn/sys/update_aabbs.hpp"
#include "edyn/sys/update_inertias.hpp"
#include "edyn/sys/update_rotated_meshes.hpp"
#include "edyn/time/time.hpp"
#include "edyn/parallel/job_dispatcher.hpp"
#include "edyn/parallel/island_delta_builder.hpp"
#include "edyn/parallel/message.hpp"
#include "edyn/serialization/memory_archive.hpp"
#include "edyn/constraints/constraint_impulse.hpp"
#include "edyn/comp/dirty.hpp"
#include "edyn/comp/graph_node.hpp"
#include "edyn/comp/graph_edge.hpp"
#include "edyn/comp/rotated_mesh_list.hpp"
#include "edyn/math/constants.hpp"
#include "edyn/util/aabb_util.hpp"
#include "edyn/util/rigidbody.hpp"
#include "edyn/util/vector.hpp"
#include "edyn/util/collision_util.hpp"
#include "edyn/context/settings.hpp"
#include <entt/entity/utility.hpp>
#include <memory>
#include <variant>
#include <set>
#include <entt/entity/registry.hpp>

namespace edyn {

void island_worker_func(job::data_type &data) {
    auto archive = memory_input_archive(data.data(), data.size());
    intptr_t worker_intptr;
    archive(worker_intptr);
    auto *worker = reinterpret_cast<island_worker *>(worker_intptr);

    if (worker->is_terminating()) {
        // `worker` is dynamically allocated and must be manually deallocated
        // when it terminates.
        worker->do_terminate();
        delete worker;
    } else {
        worker->update();
    }
}

island_worker::island_worker(const std::string &name, const settings &settings,
                             const material_mix_table &material_table, message_queue_identifier coordinator_queue_id)
    : m_state(state::init)
    , m_bphase(m_registry)
    , m_nphase(m_registry)
    , m_solver(m_registry)
    , m_delta_builder((*settings.make_island_delta_builder)())
    , m_importing_delta(false)
    , m_destroying_node(false)
    , m_transferring_island(false)
    , m_message_queue(message_dispatcher::global().make_queue<
        msg::set_paused,
        msg::set_settings,
        msg::step_simulation,
        msg::set_com,
        msg::set_material_table,
        msg::update_entities,
        msg::transfer_island_request,
        msg::transfer_island>(name.c_str()))
    , m_coordinator_queue_id(coordinator_queue_id)
{
    m_registry.set<entity_graph>();
    m_registry.set<edyn::settings>(settings);
    m_registry.set<material_mix_table>(material_table);

    // Avoid multi-threading issues in the `should_collide` function by
    // pre-allocating the pools required in there.
    m_registry.prepare<collision_filter>();
    m_registry.prepare<collision_exclusion>();

    m_this_job.func = &island_worker_func;
    auto archive = fixed_memory_output_archive(m_this_job.data.data(), m_this_job.data.size());
    auto ctx_intptr = reinterpret_cast<intptr_t>(this);
    archive(ctx_intptr);

    m_last_time = performance_time();

    m_message_queue.push_sink().connect<&island_worker::reschedule>(*this);
}

island_worker::~island_worker() = default;

void island_worker::init() {
    m_registry.on_construct<graph_node>().connect<&island_worker::on_construct_graph_node>(*this);
    m_registry.on_construct<graph_edge>().connect<&island_worker::on_construct_graph_edge>(*this);
    m_registry.on_destroy<graph_node>().connect<&island_worker::on_destroy_graph_node>(*this);
    m_registry.on_destroy<graph_edge>().connect<&island_worker::on_destroy_graph_edge>(*this);
    m_registry.on_destroy<island_resident>().connect<&island_worker::on_destroy_island_resident>(*this);
    m_registry.on_destroy<contact_manifold>().connect<&island_worker::on_destroy_contact_manifold>(*this);
    m_registry.on_destroy<contact_point>().connect<&island_worker::on_destroy_contact_point>(*this);
    m_registry.on_construct<polyhedron_shape>().connect<&island_worker::on_construct_polyhedron_shape>(*this);
    m_registry.on_construct<compound_shape>().connect<&island_worker::on_construct_compound_shape>(*this);
    m_registry.on_destroy<rotated_mesh_list>().connect<&island_worker::on_destroy_rotated_mesh_list>(*this);

    m_message_queue.sink<msg::update_entities>().connect<&island_worker::on_update_entities>(*this);
    m_message_queue.sink<msg::transfer_island_request>().connect<&island_worker::on_transfer_island_request>(*this);
    m_message_queue.sink<msg::transfer_island>().connect<&island_worker::on_transfer_island>(*this);
    m_message_queue.sink<msg::set_paused>().connect<&island_worker::on_set_paused>(*this);
    m_message_queue.sink<msg::step_simulation>().connect<&island_worker::on_step_simulation>(*this);
    m_message_queue.sink<msg::set_com>().connect<&island_worker::on_set_com>(*this);
    m_message_queue.sink<msg::set_settings>().connect<&island_worker::on_set_settings>(*this);
    m_message_queue.sink<msg::set_material_table>().connect<&island_worker::on_set_material_table>(*this);

    // Process messages enqueued before the worker was started. This includes
    // the island deltas containing the initial entities that were added to
    // this island.
    process_messages();

    init_new_nodes_and_edges();

    auto &settings = m_registry.ctx<edyn::settings>();
    if (settings.external_system_init) {
        (*settings.external_system_init)(m_registry);
    }

    // Run broadphase to initialize the internal dynamic trees with the
    // imported AABBs.
    m_bphase.update();

    m_state = state::step;
}

void island_worker::on_destroy_contact_manifold(entt::registry &registry, entt::entity entity) {
    const auto importing = m_importing_delta;
    const auto transferring = m_transferring_island;

    // If importing, do not insert this event into the delta because that means
    // the entity was already destroyed in the coordinator.
    // If transferring an island, do not add this change to the delta because
    // the entity is not actually being destroyed, it's being moved.
    if (!importing && !transferring) {
        m_delta_builder->destroyed(entity);
    }

    auto &manifold = registry.get<contact_manifold>(entity);
    auto num_points = manifold.num_points();

    for (size_t i = 0; i < num_points; ++i) {
        auto contact_entity = manifold.point[i];

        if (!importing) {
            registry.destroy(contact_entity);
        }

        if (!importing && !transferring) {
            m_delta_builder->destroyed(contact_entity);
        }

        if (m_entity_map.has_loc(contact_entity)) {
            m_entity_map.erase_loc(contact_entity);
        }
    }

    // Mapping might not yet exist if this entity was just created locally and
    // the coordinator has not yet replied back with the main entity id.
    if (m_entity_map.has_loc(entity)) {
        m_entity_map.erase_loc(entity);
    }
}

void island_worker::on_destroy_contact_point(entt::registry &registry, entt::entity entity) {
    const auto importing = m_importing_delta;
    const auto transferring = m_transferring_island;

    if (!importing && !transferring) {
        m_delta_builder->destroyed(entity);
    }

    if (m_entity_map.has_loc(entity)) {
        m_entity_map.erase_loc(entity);
    }
}

void island_worker::on_construct_graph_node(entt::registry &registry, entt::entity entity) {
    m_new_graph_nodes.push_back(entity);

    // If this node is being created as a result of importing a delta, then it's
    // expected to already have an `island_resident` component assigned.
    if (!m_importing_delta) {
        if (registry.any_of<procedural_tag>(entity)) {
            registry.emplace<island_resident>(entity);
            m_delta_builder->created<island_resident>(entity, registry);
        }
    }
}

void island_worker::on_construct_graph_edge(entt::registry &registry, entt::entity entity) {
    m_new_graph_edges.push_back(entity);

    if (!m_importing_delta) {
        // Assuming this graph edge is a constraint or contact manifold, which
        // are always procedural, thus can only reside in one island.
        registry.emplace<island_resident>(entity);
        m_delta_builder->created<island_resident>(entity, registry);
    }
}

void island_worker::on_destroy_graph_node(entt::registry &registry, entt::entity entity) {
    if (!m_transferring_island) {
        auto &node = registry.get<graph_node>(entity);
        auto &graph = registry.ctx<entity_graph>();

        m_destroying_node = true;

        graph.visit_edges(node.node_index, [&] (entt::entity edge_entity) {
            registry.destroy(edge_entity);
        });

        m_destroying_node = false;

        graph.remove_all_edges(node.node_index);
        graph.remove_node(node.node_index);
    }

    if (!m_importing_delta && !m_transferring_island) {
        m_delta_builder->destroyed(entity);
    }

    if (m_entity_map.has_loc(entity)) {
        m_entity_map.erase_loc(entity);
    }
}

void island_worker::on_destroy_graph_edge(entt::registry &registry, entt::entity entity) {
    if (!m_destroying_node && !m_transferring_island) {
        auto &edge = registry.get<graph_edge>(entity);
        registry.ctx<entity_graph>().remove_edge(edge.edge_index);
    }

    if (!m_importing_delta && !m_transferring_island) {
        m_delta_builder->destroyed(entity);
    }

    if (m_entity_map.has_loc(entity)) {
        m_entity_map.erase_loc(entity);
    }
}

void island_worker::on_destroy_island_resident(entt::registry &registry, entt::entity entity) {
    // If this is happening while an island is being transferred, it means this
    // entity is being moved out of this worker along with the island it resides
    // in. Thus, it's unnecessary to update the island components.
    if (m_transferring_island) {
        return;
    }

    auto &resident = registry.get<island_resident>(entity);
    auto &island = registry.get<edyn::island>(resident.island_entity);
    // Is this a node or an edge? Idk
    island.nodes.erase(entity);
    island.edges.erase(entity);

    auto &stats = registry.get<island_stats>(resident.island_entity);
    stats.num_nodes = island.nodes.size();
    stats.num_edges = island.edges.size();
    m_delta_builder->updated(resident.island_entity, stats);

    m_islands_to_split.insert(resident.island_entity);
}

void island_worker::on_construct_polyhedron_shape(entt::registry &registry, entt::entity entity) {
    m_new_polyhedron_shapes.push_back(entity);
}

void island_worker::on_construct_compound_shape(entt::registry &registry, entt::entity entity) {
    m_new_compound_shapes.push_back(entity);
}

void island_worker::on_destroy_rotated_mesh_list(entt::registry &registry, entt::entity entity) {
    auto &rotated = registry.get<rotated_mesh_list>(entity);
    if (rotated.next != entt::null) {
        // Cascade delete. Could lead to mega tall call stacks.
        registry.destroy(rotated.next);
    }
}

void island_worker::on_update_entities(const message<msg::update_entities> &msg) {
    import_delta(msg.content.delta, m_entity_map);
}

void island_worker::on_transfer_island_request(const message<msg::transfer_island_request> &msg) {
    auto remote_entity = msg.content.island_entity;

    if (!m_entity_map.has_rem(remote_entity)) {
        message_dispatcher::global().send<msg::island_transfer_failure>(msg.sender, message_queue_id(), remote_entity);
        return;
    }

    auto island_entity = m_entity_map.remloc(remote_entity);
    EDYN_ASSERT(m_registry.any_of<island_tag>(island_entity));

    auto &settings = m_registry.ctx<edyn::settings>();
    auto builder = (*settings.make_island_delta_builder)();
    m_transferring_island = true;

    // Package entire island into one delta and remove entities from local registry.
    auto &island = m_registry.get<edyn::island>(island_entity);
    auto &graph = m_registry.ctx<entity_graph>();
    auto node_view = m_registry.view<graph_node>();
    auto edge_view = m_registry.view<graph_edge>();
    auto manifold_view = m_registry.view<contact_manifold>();

    builder->created(island_entity);
    builder->created_all(island_entity, m_registry);
    builder->insert_entity_mapping(island_entity, m_entity_map.locrem(island_entity));

    for (auto entity : island.edges) {
        builder->created(entity);
        builder->created_all(entity, m_registry);

        if (m_entity_map.has_loc(entity)) {
            builder->insert_entity_mapping(entity, m_entity_map.locrem(entity));
        }

        if (manifold_view.contains(entity)) {
            auto [manifold] = manifold_view.get(entity);
            auto num_points = manifold.num_points();

            for (size_t i = 0; i < num_points; ++i) {
                auto contact_entity = manifold.point[i];
                builder->created(contact_entity);
                builder->created_all(contact_entity, m_registry);

                if (m_entity_map.has_loc(contact_entity)) {
                    builder->insert_entity_mapping(contact_entity, m_entity_map.locrem(contact_entity));
                }
            }
        }

        auto [edge] = edge_view.get(entity);
        graph.remove_edge(edge.edge_index);
    }

    for (auto entity : island.nodes) {
        builder->created(entity);
        builder->created_all(entity, m_registry);

        if (m_entity_map.has_loc(entity)) {
            builder->insert_entity_mapping(entity, m_entity_map.locrem(entity));
        }

        auto [node] = node_view.get(entity);
        graph.remove_node(node.node_index);
    }

    for (auto entity : island.edges) {
        m_registry.destroy(entity);
    }

    for (auto entity : island.nodes) {
        m_registry.destroy(entity);
    }

    m_registry.destroy(island_entity);

    m_transferring_island = false;

    message_dispatcher::global().send<msg::transfer_island>(
        msg.content.destination_id, message_queue_id(), builder->finish(), island_entity);
}

void island_worker::on_transfer_island(const message<msg::transfer_island> &msg) {
    auto &delta = msg.content.delta;
    auto w2w_entity_map = entity_map{}; // Temporary worker-to-worker entity map.
    auto local_entity_map = entity_map{};
    auto created_entities = std::vector<entt::entity>{};

    delta.m_entity_map.each([&] (auto coordinator_entity, auto worker_entity) {
        auto local_entity = m_registry.create();
        m_entity_map.insert(coordinator_entity, local_entity);
        w2w_entity_map.insert(worker_entity, local_entity);
        local_entity_map.insert(local_entity, coordinator_entity);
        created_entities.push_back(local_entity);
    });

    import_delta(delta, w2w_entity_map);

    auto island_entity = w2w_entity_map.remloc(msg.content.island_entity);
    auto &island = m_registry.emplace<edyn::island>(island_entity);
    auto resident_view = m_registry.view<island_resident>();

    for (auto remote_entity : delta.created_entities()) {
        auto local_entity = w2w_entity_map.remloc(remote_entity);

        if (resident_view.contains(local_entity)) {
            auto [resident] = resident_view.get(local_entity);
            resident.island_entity = island_entity;

            if (m_registry.any_of<graph_node>(local_entity)) {
                island.nodes.insert(local_entity);
            } else if (m_registry.any_of<graph_edge>(local_entity)) {
                island.edges.insert(local_entity);
            }
        }
    }

    message_dispatcher::global().send<msg::island_transfer_complete>(
        m_coordinator_queue_id, m_message_queue.identifier, created_entities, local_entity_map);
}

void island_worker::import_delta(const island_delta &delta, entity_map &entity_map) {
    m_importing_delta = true;

    delta.import(m_registry, entity_map);

    for (auto remote_entity : delta.created_entities()) {
        if (!entity_map.has_rem(remote_entity)) continue;
        auto local_entity = entity_map.remloc(remote_entity);
        m_delta_builder->insert_entity_mapping(remote_entity, local_entity);
    }

    auto &graph = m_registry.ctx<entity_graph>();
    auto node_view = m_registry.view<graph_node>();
    auto &index_source = m_delta_builder->get_index_source();

    // Insert nodes in the graph for each rigid body.
    auto insert_node = [&] (entt::entity remote_entity, auto &) {
        insert_remote_node(remote_entity, entity_map);
    };

    delta.created_for_each<dynamic_tag>(index_source, insert_node);
    delta.created_for_each<static_tag>(index_source, insert_node);
    delta.created_for_each<kinematic_tag>(index_source, insert_node);
    delta.created_for_each<external_tag>(index_source, insert_node);

    // Insert edges in the graph for contact manifolds.
    delta.created_for_each<contact_manifold>(index_source, [&] (entt::entity remote_entity, const contact_manifold &manifold) {
        if (!entity_map.has_rem(remote_entity)) return;

        auto local_entity = entity_map.remloc(remote_entity);
        auto &node0 = node_view.get<graph_node>(manifold.body[0]);
        auto &node1 = node_view.get<graph_node>(manifold.body[1]);
        auto edge_index = graph.insert_edge(local_entity, node0.node_index, node1.node_index);
        m_registry.emplace<graph_edge>(local_entity, edge_index);
    });

    // Insert edges in the graph for constraints (except contact constraints).
    delta.created_for_each(constraints_tuple, index_source, [&] (entt::entity remote_entity, const auto &con) {
        // Contact constraints are not added as edges to the graph.
        // The contact manifold which owns them is added instead.
        if constexpr(std::is_same_v<std::decay_t<decltype(con)>, contact_constraint>) return;

        if (!entity_map.has_rem(remote_entity)) return;

        auto local_entity = entity_map.remloc(remote_entity);
        auto &node0 = node_view.get<graph_node>(con.body[0]);
        auto &node1 = node_view.get<graph_node>(con.body[1]);
        auto edge_index = graph.insert_edge(local_entity, node0.node_index, node1.node_index);
        m_registry.emplace<graph_edge>(local_entity, edge_index);
    });

    // New contact points might be coming from another island after a merge or
    // split and they might not yet have a contact constraint associated with
    // them if they were just created in the last step of the island where it's
    // coming from.
    auto cp_view = m_registry.view<contact_point>();
    auto cc_view = m_registry.view<contact_constraint>();
    auto mat_view = m_registry.view<material>();
    delta.created_for_each<contact_point>(index_source, [&] (entt::entity remote_entity, const contact_point &) {
        if (!entity_map.has_rem(remote_entity)) {
            return;
        }

        auto local_entity = entity_map.remloc(remote_entity);

        if (cc_view.contains(local_entity)) {
            return;
        }

        auto &cp = cp_view.get<contact_point>(local_entity);

        if (mat_view.contains(cp.body[0]) && mat_view.contains(cp.body[1])) {
            create_contact_constraint(m_registry, local_entity, cp);
        }
    });

    // When orientation is set manually, a few dependent components must be
    // updated, e.g. AABB, cached origin, inertia_world_inv, rotated meshes...
    delta.updated_for_each<orientation>(index_source, [&] (entt::entity remote_entity, const orientation &orn) {
        if (!entity_map.has_rem(remote_entity)) return;

        auto local_entity = entity_map.remloc(remote_entity);

        if (auto *origin = m_registry.try_get<edyn::origin>(local_entity)) {
            auto &com = m_registry.get<center_of_mass>(local_entity);
            auto &pos = m_registry.get<position>(local_entity);
            *origin = to_world_space(-com, pos, orn);
        }

        if (m_registry.any_of<AABB>(local_entity)) {
            update_aabb(m_registry, local_entity);
        }

        if (m_registry.any_of<dynamic_tag>(local_entity)) {
            update_inertia(m_registry, local_entity);
        }

        if (m_registry.any_of<rotated_mesh_list>(local_entity)) {
            update_rotated_mesh(m_registry, local_entity);
        }
    });

    // When position is set manually, the AABB and cached origin must be updated.
    delta.updated_for_each<position>(index_source, [&] (entt::entity remote_entity, const position &pos) {
        if (!entity_map.has_rem(remote_entity)) return;

        auto local_entity = entity_map.remloc(remote_entity);

        if (auto *origin = m_registry.try_get<edyn::origin>(local_entity)) {
            auto &com = m_registry.get<center_of_mass>(local_entity);
            auto &orn = m_registry.get<orientation>(local_entity);
            *origin = to_world_space(-com, pos, orn);
        }

        if (m_registry.any_of<AABB>(local_entity)) {
            update_aabb(m_registry, local_entity);
        }
    });

    m_importing_delta = false;
}

void island_worker::wake_up_island(entt::entity island_entity) {
    m_registry.remove<sleeping_tag>(island_entity);
    m_delta_builder->destroyed<sleeping_tag>(island_entity);

    auto &island = m_registry.get<edyn::island>(island_entity);

    for (auto entity : island.nodes) {
        m_registry.remove<sleeping_tag>(entity);
        m_delta_builder->destroyed<sleeping_tag>(entity);
    }

    for (auto entity : island.edges) {
        m_registry.remove<sleeping_tag>(entity);
        m_delta_builder->destroyed<sleeping_tag>(entity);

        if (auto *manifold = m_registry.try_get<contact_manifold>(entity)) {
            auto num_points = manifold->num_points();

            for (size_t i = 0; i < num_points; ++i) {
                auto contact_entity = manifold->point[i];
                m_registry.remove<sleeping_tag>(contact_entity);
            }
        }
    }
}

bool island_worker::all_sleeping() {
    auto sleeping_view = m_registry.view<sleeping_tag>();
    auto island_view = m_registry.view<island_tag>();

    for (auto island_entity : island_view) {
        if (!sleeping_view.contains(island_entity)) {
            return false;
        }
    }

    return true;
}

void island_worker::sync() {
    // Always update island AABBs since the coordinator needs them to detect
    // when islands from different workers are about to interact.
    m_registry.view<island_aabb>().each([&] (entt::entity entity, island_aabb &aabb) {
        m_delta_builder->updated(entity, aabb);
    });

    // Update continuous components.
    m_registry.view<continuous>().each([&] (entt::entity entity, continuous &cont) {
        for (size_t i = 0; i < cont.size; ++i) {
            m_delta_builder->updated(entity, m_registry, cont.types[i]);
        }
    });

    sync_dirty();

    message_dispatcher::global().send<msg::step_update>(m_coordinator_queue_id, m_message_queue.identifier, m_delta_builder->finish());
}

void island_worker::sync_dirty() {
    // Assign dirty components to the delta builder. This can be called at
    // any time to move the current dirty entities into the next island delta.
    m_registry.view<dirty>().each([&] (entt::entity entity, dirty &dirty) {
        if (dirty.is_new_entity) {
            m_delta_builder->created(entity);
        }

        m_delta_builder->created(entity, m_registry,
            dirty.created_indexes.begin(), dirty.created_indexes.end());
        m_delta_builder->updated(entity, m_registry,
            dirty.updated_indexes.begin(), dirty.updated_indexes.end());
        m_delta_builder->destroyed(entity,
            dirty.destroyed_indexes.begin(), dirty.destroyed_indexes.end());
    });

    m_registry.clear<dirty>();
}

void island_worker::update() {
    switch (m_state) {
    case state::init:
        init();
        maybe_reschedule();
        break;
    case state::step:
        process_messages();

        if (should_step()) {
            begin_step();
            run_solver();
            if (run_broadphase()) {
                if (run_narrowphase()) {
                    finish_step();
                    maybe_reschedule();
                }
            }
        } else {
            maybe_reschedule();
        }

        break;
    case state::begin_step:
        begin_step();
        reschedule_now();
        break;
    case state::solve:
        run_solver();
        reschedule_now();
        break;
    case state::broadphase:
        if (run_broadphase()) {
            reschedule_now();
        }
        break;
    case state::broadphase_async:
        finish_broadphase();
        if (run_narrowphase()) {
            finish_step();
            maybe_reschedule();
        }
        break;
    case state::narrowphase:
        if (run_narrowphase()) {
            finish_step();
            maybe_reschedule();
        }
        break;
    case state::narrowphase_async:
        finish_narrowphase();
        finish_step();
        maybe_reschedule();
        break;
    case state::finish_step:
        finish_step();
        maybe_reschedule();
        break;
    }
}

void island_worker::process_messages() {
    m_message_queue.update();
}

bool island_worker::should_step() {
    auto time = performance_time();

    if (m_state == state::begin_step) {
        m_step_start_time = time;
        return true;
    }

    auto &settings = m_registry.ctx<edyn::settings>();

    if (settings.paused || all_sleeping()) {
        return false;
    }

    auto dt = time - m_last_time;

    if (dt < settings.fixed_dt) {
        return false;
    }

    m_step_start_time = time;
    m_state = state::begin_step;

    return true;
}

void island_worker::begin_step() {
    EDYN_ASSERT(m_state == state::begin_step);

    auto &settings = m_registry.ctx<edyn::settings>();
    if (settings.external_system_pre_step) {
        (*settings.external_system_pre_step)(m_registry);
    }

    // Perform splits after processing messages from coodinator and running
    // external logic, which could've destroyed nodes or edges.
    split_islands();

    init_new_nodes_and_edges();

    init_new_shapes();

    // Create new contact constraints at the beginning of the step. Since
    // contact points are created at the end of a step, creating constraints
    // at that point would mean that they'd have zero applied impulse,
    // which leads to contact point construction observers not getting the
    // value of the initial impulse of a new contact. Doing it here, means
    // that at the end of the step, the `constraint_impulse` will have the
    // value of the impulse applied and the construction of `constraint_impulse`
    // or `contact_constraint` can be observed to capture the initial impact
    // of a new contact.
    m_nphase.create_contact_constraints();

    m_state = state::solve;
}

void island_worker::run_solver() {
    EDYN_ASSERT(m_state == state::solve);
    m_solver.update(m_registry.ctx<edyn::settings>().fixed_dt);
    m_state = state::broadphase;
}

bool island_worker::run_broadphase() {
    EDYN_ASSERT(m_state == state::broadphase);

    if (m_bphase.parallelizable()) {
        m_state = state::broadphase_async;
        m_bphase.update_async(m_this_job);
        return false;
    } else {
        m_bphase.update();
        m_state = state::narrowphase;
        return true;
    }
}

void island_worker::finish_broadphase() {
    EDYN_ASSERT(m_state == state::broadphase_async);
    m_bphase.finish_async_update();
    m_state = state::narrowphase;
}

bool island_worker::run_narrowphase() {
    EDYN_ASSERT(m_state == state::narrowphase);

    // Narrow-phase is run right after broad-phase, where manifolds could have
    // been destroyed, potentially causing islands to split. Thus, split any
    // pending islands before proceeding.
    split_islands();

    if (m_nphase.parallelizable()) {
        m_state = state::narrowphase_async;
        m_nphase.update_async(m_this_job);
        return false;
    } else {
        // Separating contact points will be destroyed in the next call. Move
        // the dirty contact points into the island delta before that happens
        // because the dirty component is removed as well, which would cause
        // points that were created in this step and are going to be destroyed
        // next to be missing in the island delta.
        sync_dirty();
        m_nphase.update();
        m_state = state::finish_step;
        return true;
    }
}

void island_worker::finish_narrowphase() {
    EDYN_ASSERT(m_state == state::narrowphase_async);
    // In the asynchronous narrow-phase update, separating contact points will
    // be destroyed in the next call. Following the same logic as above, move
    // the dirty contact points into the current island delta before that
    // happens.
    sync_dirty();
    m_nphase.finish_async_update();
    m_state = state::finish_step;
}

void island_worker::finish_step() {
    EDYN_ASSERT(m_state == state::finish_step);

    auto dt = m_step_start_time - m_last_time;

    // Set a limit on the number of steps the worker can lag behind the current
    // time to prevent it from getting stuck in the past in case of a
    // substantial slowdown.
    auto &settings = m_registry.ctx<edyn::settings>();
    const auto fixed_dt = settings.fixed_dt;

    constexpr int max_lagging_steps = 10;
    auto num_steps = int(std::floor(dt / fixed_dt));

    if (num_steps > max_lagging_steps) {
        auto remainder = dt - num_steps * fixed_dt;
        m_last_time = m_step_start_time - (remainder + max_lagging_steps * fixed_dt);
    } else {
        m_last_time += fixed_dt;
    }

    for (auto island_entity : m_registry.view<island_tag>(entt::exclude_t<sleeping_tag>{})) {
        maybe_go_to_sleep(island_entity);
    }

    if (settings.external_system_post_step) {
        (*settings.external_system_post_step)(m_registry);
    }

    sync();

    m_state = state::step;
}

void island_worker::reschedule_now() {
    job_dispatcher::global().async(m_this_job);
}

void island_worker::maybe_reschedule() {
    // Reschedule this job only if not paused nor sleeping.
    auto paused = m_registry.ctx<edyn::settings>().paused;

    // The update is done and this job can be rescheduled after this point
    auto reschedule_count = m_reschedule_counter.exchange(0, std::memory_order_acq_rel);
    EDYN_ASSERT(reschedule_count != 0);

    // If the number of reschedule requests is greater than one, it means there
    // are external requests involved, not just the normal internal reschedule.
    // Always reschedule for immediate execution in that case.
    if (reschedule_count == 1) {
        if (!paused && !all_sleeping()) {
            reschedule_later();
        }
    } else {
        reschedule();
    }
}

void island_worker::reschedule_later() {
    // Only reschedule if it has not been scheduled and updated already.
    auto reschedule_count = m_reschedule_counter.fetch_add(1, std::memory_order_acq_rel);
    if (reschedule_count > 0) return;

    // If the timestamp of the current registry state is more that `m_fixed_dt`
    // before the current time, schedule it to run at a later time.
    auto time = performance_time();
    auto fixed_dt = m_registry.ctx<edyn::settings>().fixed_dt;
    auto delta_time = m_last_time + fixed_dt - time;

    if (delta_time > 0) {
        job_dispatcher::global().async_after(delta_time, m_this_job);
    } else {
        job_dispatcher::global().async(m_this_job);
    }
}

void island_worker::reschedule() {
    // Only reschedule if it has not been scheduled and updated already.
    auto reschedule_count = m_reschedule_counter.fetch_add(1, std::memory_order_acq_rel);
    if (reschedule_count > 0) return;

    job_dispatcher::global().async(m_this_job);
}

void island_worker::init_new_nodes_and_edges() {
    if (m_new_graph_nodes.empty() && m_new_graph_edges.empty()) return;

    auto &graph = m_registry.ctx<entity_graph>();
    auto node_view = m_registry.view<graph_node>();
    auto edge_view = m_registry.view<graph_edge>();
    std::set<entity_graph::index_type> procedural_node_indices;

    for (auto entity : m_new_graph_nodes) {
        if (m_registry.any_of<procedural_tag>(entity)) {
            auto &node = node_view.get<graph_node>(entity);
            procedural_node_indices.insert(node.node_index);
        }
    }

    for (auto edge_entity : m_new_graph_edges) {
        auto &edge = edge_view.get<graph_edge>(edge_entity);
        auto node_entities = graph.edge_node_entities(edge.edge_index);

        if (m_registry.any_of<procedural_tag>(node_entities.first)) {
            auto &node = node_view.get<graph_node>(node_entities.first);
            procedural_node_indices.insert(node.node_index);
        }

        if (m_registry.any_of<procedural_tag>(node_entities.second)) {
            auto &node = node_view.get<graph_node>(node_entities.second);
            procedural_node_indices.insert(node.node_index);
        }
    }

    m_new_graph_nodes.clear();
    m_new_graph_edges.clear();

    if (procedural_node_indices.empty()) return;

    std::vector<entt::entity> connected_nodes;
    std::vector<entt::entity> connected_edges;
    std::vector<entt::entity> island_entities;
    auto resident_view = m_registry.view<island_resident>();
    auto procedural_view = m_registry.view<procedural_tag>();

    graph.reach(
        procedural_node_indices.begin(), procedural_node_indices.end(),
        [&] (entt::entity entity) { // visitNodeFunc
            // We only visit procedural nodes.
            EDYN_ASSERT(procedural_view.contains(entity));

            if (resident_view.get<island_resident>(entity).island_entity == entt::null) {
                connected_nodes.push_back(entity);
            }
        },
        [&] (entt::entity entity) { // visitEdgeFunc
            auto &edge_resident = resident_view.get<island_resident>(entity);

            if (edge_resident.island_entity == entt::null) {
                connected_edges.push_back(entity);
            } else {
                auto contains_island = vector_contains(island_entities, edge_resident.island_entity);

                if (!contains_island) {
                    island_entities.push_back(edge_resident.island_entity);
                }
            }
        },
        [&] (entity_graph::index_type node_index) { // shouldVisitFunc
            auto other_entity = graph.node_entity(node_index);

            if (!procedural_view.contains(other_entity)) {
                return false;
            }

            // Visit neighbor node if it's not in an island yet.
            auto &other_resident = resident_view.get<island_resident>(other_entity);

            if (other_resident.island_entity == entt::null) {
                return true;
            }

            auto contains_island = vector_contains(island_entities, other_resident.island_entity);

            // Collect islands involved in this connected component.
            if (!contains_island) {
                island_entities.push_back(other_resident.island_entity);
            }

            bool continue_visiting = false;

            // Visit neighbor if it contains an edge that is not in an island yet.
            graph.visit_edges(node_index, [&] (entt::entity edge_entity) {
                if (resident_view.get<island_resident>(edge_entity).island_entity == entt::null) {
                    continue_visiting = true;
                }
            });

            return continue_visiting;
        },
        [&] () { // connectedComponentFunc
            if (island_entities.size() <= 1) {
                // Assign island to the residents.
                auto island_entity = entt::entity{};

                if (island_entities.empty()) {
                    island_entity = create_island();
                } else {
                    island_entity = island_entities.front();
                }

                insert_to_island(island_entity, connected_nodes, connected_edges);
            } else {
                // Islands have to be merged.
                merge_islands(island_entities, connected_nodes, connected_edges);
            }

            connected_nodes.clear();
            connected_edges.clear();
            island_entities.clear();
        });
}

entt::entity island_worker::create_island() {
    auto island_entity = m_registry.create();
    m_registry.emplace<island>(island_entity);
    m_registry.emplace<island_aabb>(island_entity);
    m_registry.emplace<island_tag>(island_entity);
    m_registry.emplace<island_stats>(island_entity);
    m_delta_builder->created(island_entity);
    m_delta_builder->created_all(island_entity, m_registry);
    return island_entity;
}

void island_worker::insert_to_island(entt::entity island_entity,
                                     const std::vector<entt::entity> &nodes,
                                     const std::vector<entt::entity> &edges) {
    auto resident_view = m_registry.view<island_resident>();

    for (auto entity : nodes) {
        auto &resident = resident_view.get<island_resident>(entity);
        resident.island_entity = island_entity;
        m_delta_builder->updated(entity, resident);
    }

    for (auto entity : edges) {
        auto &resident = resident_view.get<island_resident>(entity);
        resident.island_entity = island_entity;
        m_delta_builder->updated(entity, resident);
    }

    auto &island = m_registry.get<edyn::island>(island_entity);
    island.nodes.insert(nodes.begin(), nodes.end());
    island.edges.insert(edges.begin(), edges.end());

    auto &stats = m_registry.get<island_stats>(island_entity);
    stats.num_nodes = island.nodes.size();
    stats.num_edges = island.edges.size();
    m_delta_builder->updated(island_entity, stats);

    wake_up_island(island_entity);
}

void island_worker::merge_islands(const std::vector<entt::entity> &island_entities,
                                  const std::vector<entt::entity> &new_nodes,
                                  const std::vector<entt::entity> &new_edges) {
    EDYN_ASSERT(island_entities.size() > 1);

    // Pick biggest island and move the other entities into it.
    auto island_entity = entt::entity{entt::null};
    size_t biggest_size = 0;
    auto island_view = m_registry.view<island>();

    for (auto entity : island_entities) {
        auto &island = island_view.get<edyn::island>(entity);
        auto size = island.nodes.size() + island.edges.size();

        if (size > biggest_size) {
            biggest_size = size;
            island_entity = entity;
        }
    }

    EDYN_ASSERT(island_entity != entt::null);

    auto other_island_entities = island_entities;
    vector_erase(other_island_entities, island_entity);

    auto all_nodes = new_nodes;
    auto all_edges = new_edges;

    for (auto other_island_entity : other_island_entities) {
        auto &island = island_view.get<edyn::island>(other_island_entity);
        all_nodes.insert(all_nodes.end(), island.nodes.begin(), island.nodes.end());
        all_edges.insert(all_edges.end(), island.edges.begin(), island.edges.end());
    }

    insert_to_island(island_entity, all_nodes, all_edges);

    // Destroy empty islands.
    m_registry.destroy(other_island_entities.begin(), other_island_entities.end());

    for (auto entity : other_island_entities) {
        m_delta_builder->destroyed(entity);
    }
}

void island_worker::split_islands() {
    if (m_islands_to_split.empty()) return;

    auto island_view = m_registry.view<island>();
    auto node_view = m_registry.view<graph_node>();
    auto resident_view = m_registry.view<island_resident>();
    auto aabb_view = m_registry.view<AABB>();
    auto &graph = m_registry.ctx<entity_graph>();
    auto connected_nodes = std::vector<entt::entity>{};
    auto connected_edges = std::vector<entt::entity>{};

    for (auto island_entity : m_islands_to_split) {
        auto &island = island_view.get<edyn::island>(island_entity);

        if (island.nodes.empty()) {
            EDYN_ASSERT(island.edges.empty());
            m_registry.destroy(island_entity);
            m_delta_builder->destroyed(island_entity);
            continue;
        }

        // Traverse graph starting at any of the island's nodes and check if the
        // collected nodes of the connected components match the island's nodes.
        connected_nodes.clear();
        connected_edges.clear();
        auto start_node = node_view.get<graph_node>(*island.nodes.begin());

        graph.traverse_connecting_nodes(start_node.node_index, [&] (auto node_index) {
            auto node_entity = graph.node_entity(node_index);
            connected_nodes.push_back(node_entity);
        }, [&] (auto edge_index) {
            auto edge_entity = graph.edge_entity(edge_index);
            connected_edges.push_back(edge_entity);
        });

        if (island.nodes.size() == connected_nodes.size()) {
            continue;
        }

        wake_up_island(island_entity);

        // Traverse graph starting at the remaining nodes to find the other
        // connected components and create new islands for them.
        auto all_nodes = island.nodes;
        island.nodes = {connected_nodes.begin(), connected_nodes.end()};
        island.edges = {connected_edges.begin(), connected_edges.end()};

        auto &stats = m_registry.get<island_stats>(island_entity);
        stats.num_nodes = island.nodes.size();
        stats.num_edges = island.edges.size();
        m_delta_builder->updated(island_entity, stats);

        for (auto entity : connected_nodes) {
            all_nodes.erase(entity);
        }

        while (!all_nodes.empty()) {
            connected_nodes.clear();

            auto island_entity = m_registry.create();
            auto &island = m_registry.emplace<edyn::island>(island_entity);
            auto &aabb = m_registry.emplace<island_aabb>(island_entity);
            auto &stats = m_registry.emplace<island_stats>(island_entity);
            m_registry.emplace<island_tag>(island_entity);

            auto start_node = node_view.get<graph_node>(*all_nodes.begin());
            auto is_first_node = true;

            graph.traverse_connecting_nodes(start_node.node_index,
                [&] (auto node_index) {
                    auto node_entity = graph.node_entity(node_index);
                    island.nodes.insert(node_entity);

                    auto &resident = resident_view.get<island_resident>(node_entity);
                    resident.island_entity = island_entity;
                    m_delta_builder->updated(node_entity, resident);

                    if (aabb_view.contains(node_entity)) {
                        auto &node_aabb = aabb_view.get<AABB>(node_entity);

                        if (is_first_node) {
                            aabb = {node_aabb};
                            is_first_node = false;
                        } else {
                            aabb = {enclosing_aabb(aabb, node_aabb)};
                        }
                    }

                    connected_nodes.push_back(node_entity);
                }, [&] (auto edge_index) {
                    auto edge_entity = graph.edge_entity(edge_index);
                    island.edges.insert(edge_entity);

                    auto &resident = resident_view.get<island_resident>(edge_entity);
                    resident.island_entity = island_entity;
                    m_delta_builder->updated(edge_entity, resident);
                });

            for (auto entity : connected_nodes) {
                all_nodes.erase(entity);
            }

            stats.num_nodes = island.nodes.size();
            stats.num_edges = island.edges.size();

            m_delta_builder->created(island_entity);
            m_delta_builder->created_all(island_entity, m_registry);
        }
    }

    m_islands_to_split.clear();
}

void island_worker::init_new_shapes() {
    auto orn_view = m_registry.view<orientation>();
    auto polyhedron_view = m_registry.view<polyhedron_shape>();
    auto compound_view = m_registry.view<compound_shape>();

    for (auto entity : m_new_polyhedron_shapes) {
        if (!polyhedron_view.contains(entity)) continue;

        auto &polyhedron = polyhedron_view.get<polyhedron_shape>(entity);
        // A new `rotated_mesh` is assigned to it, replacing another reference
        // that could be already in there, thus preventing concurrent access.
        auto rotated = make_rotated_mesh(*polyhedron.mesh, orn_view.get<orientation>(entity));
        auto rotated_ptr = std::make_unique<rotated_mesh>(std::move(rotated));
        polyhedron.rotated = rotated_ptr.get();
        m_registry.emplace<rotated_mesh_list>(entity, polyhedron.mesh, std::move(rotated_ptr));
    }

    for (auto entity : m_new_compound_shapes) {
        if (!compound_view.contains(entity)) continue;

        auto &compound = compound_view.get<compound_shape>(entity);
        auto &orn = orn_view.get<orientation>(entity);
        auto prev_rotated_entity = entt::entity{entt::null};

        for (auto &node : compound.nodes) {
            if (!std::holds_alternative<polyhedron_shape>(node.shape_var)) continue;

            // Assign a `rotated_mesh_list` to this entity for the first
            // polyhedron and link it with more rotated meshes for the
            // remaining polyhedrons.
            auto &polyhedron = std::get<polyhedron_shape>(node.shape_var);
            auto local_orn = orn * node.orientation;
            auto rotated = make_rotated_mesh(*polyhedron.mesh, local_orn);
            auto rotated_ptr = std::make_unique<rotated_mesh>(std::move(rotated));
            polyhedron.rotated = rotated_ptr.get();

            if (prev_rotated_entity == entt::null) {
                m_registry.emplace<rotated_mesh_list>(entity, polyhedron.mesh, std::move(rotated_ptr), node.orientation);
                prev_rotated_entity = entity;
            } else {
                auto next = m_registry.create();
                m_registry.emplace<rotated_mesh_list>(next, polyhedron.mesh, std::move(rotated_ptr), node.orientation);

                auto &prev_rotated_list = m_registry.get<rotated_mesh_list>(prev_rotated_entity);
                prev_rotated_list.next = next;
                prev_rotated_entity = next;
            }
        }
    }

    m_new_polyhedron_shapes.clear();
    m_new_compound_shapes.clear();
}

void island_worker::insert_remote_node(entt::entity remote_entity, const entity_map &entity_map) {
    if (!entity_map.has_rem(remote_entity)) return;

    auto local_entity = entity_map.remloc(remote_entity);
    auto non_connecting = !m_registry.any_of<procedural_tag>(local_entity);

    auto &graph = m_registry.ctx<entity_graph>();
    auto node_index = graph.insert_node(local_entity, non_connecting);
    m_registry.emplace<graph_node>(local_entity, node_index);
}

void island_worker::maybe_go_to_sleep(entt::entity island_entity) {
    auto &island = m_registry.get<edyn::island>(island_entity);
    auto is_sleeping = m_registry.any_of<sleeping_tag>(island_entity);

    if (is_sleeping) {
        return;
    }

    if (could_go_to_sleep(island_entity)) {
        if (!island.sleep_timestamp) {
            island.sleep_timestamp = m_last_time;
        } else {
            auto sleep_dt = m_last_time - *island.sleep_timestamp;
            if (sleep_dt > island_time_to_sleep) {
                put_to_sleep(island_entity);
                island.sleep_timestamp.reset();
            }
        }
    } else {
        island.sleep_timestamp.reset();
    }
}

bool island_worker::could_go_to_sleep(entt::entity island_entity) const {
    auto &island = m_registry.get<edyn::island>(island_entity);
    auto sleeping_disabled_view = m_registry.view<sleeping_disabled_tag>();

    // If any entity has a `sleeping_disabled_tag` then the island should
    // not go to sleep, since the movement of all entities depend on one
    // another in the same island.
    for (auto entity : island.nodes) {
        if (sleeping_disabled_view.contains(entity)) {
            return false;
        }
    }

    // Check if there are any entities moving faster than the sleep threshold.
    auto vel_view = m_registry.view<linvel, angvel, procedural_tag>();

    for (auto entity : island.nodes) {
        auto [v, w] = vel_view.get<const linvel, const angvel>(entity);

        if ((length_sqr(v) > island_linear_sleep_threshold * island_linear_sleep_threshold) ||
            (length_sqr(w) > island_angular_sleep_threshold * island_angular_sleep_threshold)) {
            return false;
        }
    }

    return true;
}

void island_worker::put_to_sleep(entt::entity island_entity) {
    m_registry.emplace<sleeping_tag>(island_entity);
    m_delta_builder->created(island_entity, sleeping_tag{});

    auto &island = m_registry.get<edyn::island>(island_entity);

    // Assign `sleeping_tag` to all entities.
    for (auto entity : island.nodes) {
        if (auto *v = m_registry.try_get<linvel>(entity); v) {
            *v = vector3_zero;
            m_delta_builder->updated(entity, *v);
        }

        if (auto *w = m_registry.try_get<angvel>(entity); w) {
            *w = vector3_zero;
            m_delta_builder->updated(entity, *w);
        }

        m_registry.emplace<sleeping_tag>(entity);
        m_delta_builder->created(entity, sleeping_tag{});
    }

    for (auto entity : island.edges) {
        m_registry.emplace<sleeping_tag>(entity);
        m_delta_builder->created(entity, sleeping_tag{});
    }
}

void island_worker::on_set_paused(const message<msg::set_paused> &msg) {
    m_registry.ctx<edyn::settings>().paused = msg.content.paused;
    m_last_time = performance_time();
}

void island_worker::on_step_simulation(const message<msg::step_simulation> &) {
    if (!all_sleeping()) {
        m_state = state::begin_step;
    }
}

void island_worker::on_set_settings(const message<msg::set_settings> &msg) {
    m_registry.ctx<settings>() = msg.content.settings;
}

void island_worker::on_set_material_table(const message<msg::set_material_table> &msg) {
    m_registry.ctx<material_mix_table>() = msg.content.table;
}

void island_worker::on_set_com(const message<msg::set_com> &msg) {
    auto entity = m_entity_map.remloc(msg.content.entity);
    apply_center_of_mass(m_registry, entity, msg.content.com);
}

bool island_worker::is_terminated() const {
    return m_terminated.load(std::memory_order_acquire);
}

bool island_worker::is_terminating() const {
    return m_terminating.load(std::memory_order_acquire);
}

void island_worker::terminate() {
    m_terminating.store(true, std::memory_order_release);
    reschedule();
}

void island_worker::do_terminate() {
    {
        auto lock = std::lock_guard(m_terminate_mutex);
        m_terminated.store(true, std::memory_order_release);
    }
    m_terminate_cv.notify_one();
}

void island_worker::join() {
    auto lock = std::unique_lock(m_terminate_mutex);
    m_terminate_cv.wait(lock, [&] { return is_terminated(); });
}

}
