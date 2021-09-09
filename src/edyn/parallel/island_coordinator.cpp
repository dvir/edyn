#include "edyn/parallel/island_coordinator.hpp"
#include "edyn/collision/contact_manifold.hpp"
#include "edyn/collision/contact_point.hpp"
#include "edyn/comp/inertia.hpp"
#include "edyn/comp/island.hpp"
#include "edyn/comp/present_orientation.hpp"
#include "edyn/comp/present_position.hpp"
#include "edyn/comp/tag.hpp"
#include "edyn/comp/shape_index.hpp"
#include "edyn/comp/tree_resident.hpp"
#include "edyn/parallel/message.hpp"
#include "edyn/shapes/shapes.hpp"
#include "edyn/config/config.h"
#include "edyn/constraints/constraint.hpp"
#include "edyn/constraints/contact_constraint.hpp"
#include "edyn/constraints/constraint_impulse.hpp"
#include "edyn/parallel/island_delta.hpp"
#include "edyn/parallel/island_worker.hpp"
#include "edyn/comp/dirty.hpp"
#include "edyn/time/time.hpp"
#include "edyn/parallel/entity_graph.hpp"
#include "edyn/parallel/parallel_for.hpp"
#include "edyn/comp/graph_node.hpp"
#include "edyn/comp/graph_edge.hpp"
#include "edyn/util/vector.hpp"
#include "edyn/context/settings.hpp"
#include "edyn/dynamics/material_mixing.hpp"
#include "edyn//config/config.h"
#include <entt/entity/registry.hpp>
#include <set>

namespace edyn {

island_coordinator::island_coordinator(entt::registry &registry)
    : m_registry(&registry)
{
    registry.on_construct<graph_node>().connect<&island_coordinator::on_construct_graph_node>(*this);
    registry.on_destroy<graph_node>().connect<&island_coordinator::on_destroy_graph_node>(*this);
    registry.on_construct<graph_edge>().connect<&island_coordinator::on_construct_graph_edge>(*this);
    registry.on_destroy<graph_edge>().connect<&island_coordinator::on_destroy_graph_edge>(*this);

    registry.on_destroy<island_worker_resident>().connect<&island_coordinator::on_destroy_island_worker_resident>(*this);
    registry.on_destroy<multi_island_worker_resident>().connect<&island_coordinator::on_destroy_multi_island_worker_resident>(*this);

    // Add tree nodes for islands and for static and kinematic entities when
    // they're created.
    registry.on_construct<island_aabb>().connect<&island_coordinator::on_construct_island_aabb>(*this);
    registry.on_construct<static_tag>().connect<&island_coordinator::on_construct_static_kinematic_tag>(*this);
    registry.on_construct<kinematic_tag>().connect<&island_coordinator::on_construct_static_kinematic_tag>(*this);
    registry.on_destroy<tree_resident>().connect<&island_coordinator::on_destroy_tree_resident>(*this);

    create_worker();
}

island_coordinator::~island_coordinator() {
    for (auto &pair : m_island_ctx_map) {
        auto &ctx = pair.second;
        ctx->terminate();
    }
}

void island_coordinator::on_construct_graph_node(entt::registry &registry, entt::entity entity) {
    if (m_importing_delta) return;

    m_new_graph_nodes.push_back(entity);

    if (registry.any_of<procedural_tag>(entity)) {
        registry.emplace<island_resident>(entity);
        registry.emplace<island_worker_resident>(entity);
    } else {
        registry.emplace<multi_island_worker_resident>(entity);
    }
}

void island_coordinator::on_construct_graph_edge(entt::registry &registry, entt::entity entity) {
    if (m_importing_delta) return;

    m_new_graph_edges.push_back(entity);
    // Assuming this graph edge is a constraint or contact manifold, which
    // are always procedural, thus can only reside in one island.
    registry.emplace<island_resident>(entity);
    registry.emplace<island_worker_resident>(entity);
}

void island_coordinator::on_destroy_graph_node(entt::registry &registry, entt::entity entity) {
    auto &node = registry.get<graph_node>(entity);
    auto &graph = registry.ctx<entity_graph>();

    // Prevent edges from being removed in `on_destroy_graph_edge`. The more
    // direct `entity_graph::remove_all_edges` will be used instead.
    registry.on_destroy<graph_edge>().disconnect<&island_coordinator::on_destroy_graph_edge>(*this);

    graph.visit_edges(node.node_index, [&] (entt::entity edge_entity) {
        registry.destroy(edge_entity);
    });

    registry.on_destroy<graph_edge>().connect<&island_coordinator::on_destroy_graph_edge>(*this);

    graph.remove_all_edges(node.node_index);
    graph.remove_node(node.node_index);
}

void island_coordinator::on_destroy_graph_edge(entt::registry &registry, entt::entity entity) {
    auto &edge = registry.get<graph_edge>(entity);
    registry.ctx<entity_graph>().remove_edge(edge.edge_index);
}

void island_coordinator::on_destroy_island_worker_resident(entt::registry &registry, entt::entity entity) {
    auto &resident = registry.get<island_worker_resident>(entity);

    // Remove from island.
    auto &ctx = m_island_ctx_map.at(resident.worker_entity);
    ctx->m_nodes.erase(entity);
    ctx->m_edges.erase(entity);

    if (m_importing_delta) return;

    // When importing delta, the entity is removed from the entity map as part
    // of the import process. Otherwise, the removal has to be done here.
    if (ctx->m_entity_map.has_loc(entity)) {
        ctx->m_entity_map.erase_loc(entity);
    }

    // Notify the worker of the destruction which happened in the main registry
    // first.
    ctx->m_delta_builder->destroyed(entity);
}

void island_coordinator::on_destroy_multi_island_worker_resident(entt::registry &registry, entt::entity entity) {
    auto &resident = registry.get<multi_island_worker_resident>(entity);

    // Remove from islands.
    for (auto worker_entity : resident.worker_entities) {
        auto &ctx = m_island_ctx_map.at(worker_entity);
        ctx->m_nodes.erase(entity);

        if (!m_importing_delta)  {
            ctx->m_delta_builder->destroyed(entity);
        }
    }
}

void island_coordinator::on_construct_island_aabb(entt::registry &registry, entt::entity entity) {
    auto &aabb = registry.get<island_aabb>(entity);
    auto id = m_island_tree.create(aabb, entity);
    registry.emplace<tree_resident>(entity, id, true);
}

void island_coordinator::on_construct_static_kinematic_tag(entt::registry &registry, entt::entity entity) {
    if (!registry.any_of<AABB>(entity)) return;

    auto &aabb = registry.get<AABB>(entity);
    auto id = m_np_tree.create(aabb, entity);
    registry.emplace<tree_resident>(entity, id, false);
}

void island_coordinator::on_destroy_tree_resident(entt::registry &registry, entt::entity entity) {
    auto &node = registry.get<tree_resident>(entity);

    if (node.procedural) {
        m_island_tree.destroy(node.id);
    } else {
        m_np_tree.destroy(node.id);
    }
}

void island_coordinator::init_new_nodes_and_edges() {
    if (m_new_graph_nodes.empty() && m_new_graph_edges.empty()) return;

    auto &graph = m_registry->ctx<entity_graph>();
    auto node_view = m_registry->view<graph_node>();
    auto edge_view = m_registry->view<graph_edge>();
    std::set<entity_graph::index_type> procedural_node_indices;

    for (auto entity : m_new_graph_nodes) {
        if (m_registry->any_of<procedural_tag>(entity)) {
            auto &node = node_view.get<graph_node>(entity);
            procedural_node_indices.insert(node.node_index);
        } else {
            init_new_non_procedural_node(entity);
        }
    }

    for (auto edge_entity : m_new_graph_edges) {
        auto &edge = edge_view.get<graph_edge>(edge_entity);
        auto node_entities = graph.edge_node_entities(edge.edge_index);

        if (m_registry->any_of<procedural_tag>(node_entities.first)) {
            auto &node = node_view.get<graph_node>(node_entities.first);
            procedural_node_indices.insert(node.node_index);
        }

        if (m_registry->any_of<procedural_tag>(node_entities.second)) {
            auto &node = node_view.get<graph_node>(node_entities.second);
            procedural_node_indices.insert(node.node_index);
        }
    }

    m_new_graph_nodes.clear();
    m_new_graph_edges.clear();

    if (procedural_node_indices.empty()) return;

    std::vector<entt::entity> connected_nodes;
    std::vector<entt::entity> connected_edges;
    std::vector<entt::entity> worker_entities;
    auto resident_view = m_registry->view<island_worker_resident>();
    auto procedural_view = m_registry->view<procedural_tag>();

    graph.reach(
        procedural_node_indices.begin(), procedural_node_indices.end(),
        [&] (entt::entity entity) { // visitNodeFunc
            // Always add non-procedurals to the connected component.
            // Only add procedural if it's not in an island yet.
            auto is_procedural = procedural_view.contains(entity);

            if (!is_procedural ||
                (is_procedural && resident_view.get<island_worker_resident>(entity).worker_entity == entt::null)) {
                connected_nodes.push_back(entity);
            }
        },
        [&] (entt::entity entity) { // visitEdgeFunc
            auto &edge_resident = resident_view.get<island_worker_resident>(entity);

            if (edge_resident.worker_entity == entt::null) {
                connected_edges.push_back(entity);
            } else {
                auto contains_island = vector_contains(worker_entities, edge_resident.worker_entity);

                if (!contains_island) {
                    worker_entities.push_back(edge_resident.worker_entity);
                }
            }
        },
        [&] (entity_graph::index_type node_index) { // shouldVisitFunc
            auto other_entity = graph.node_entity(node_index);

            // Collect islands involved in this connected component.
            // Always visit the non-procedural nodes. Their edges won't be
            // visited later because in the graph they're non-connecting nodes.
            if (!procedural_view.contains(other_entity)) {
                return true;
            }

            // Visit neighbor node if it's not in an island yet.
            auto &other_resident = resident_view.get<island_worker_resident>(other_entity);

            if (other_resident.worker_entity == entt::null) {
                return true;
            }

            auto contains_island = vector_contains(worker_entities, other_resident.worker_entity);

            if (!contains_island) {
                worker_entities.push_back(other_resident.worker_entity);
            }

            bool continue_visiting = false;

            // Visit neighbor if it contains an edge that is not in an island yet.
            graph.visit_edges(node_index, [&] (entt::entity edge_entity) {
                if (resident_view.get<island_worker_resident>(edge_entity).worker_entity == entt::null) {
                    continue_visiting = true;
                }
            });

            return continue_visiting;
        },
        [&] () { // connectedComponentFunc
            if (worker_entities.empty()) {
                // TODO: select least busy worker to insert new nodes in.
                auto &worker = *m_island_ctx_map.begin()->second;
                insert_to_worker(worker, connected_nodes, connected_edges);
            } else if (worker_entities.size() == 1) {
                auto worker_entity = *worker_entities.begin();
                insert_to_worker(worker_entity, connected_nodes, connected_edges);
            } else {
                // TODO: move islands into a single worker and then create nodes and edges
                // after acknowledgement.
                //merge_islands(island_entities, connected_nodes, connected_edges);
            }

            connected_nodes.clear();
            connected_edges.clear();
            worker_entities.clear();
        });
}

void island_coordinator::init_new_non_procedural_node(entt::entity node_entity) {
    EDYN_ASSERT(!(m_registry->any_of<procedural_tag>(node_entity)));

    auto procedural_view = m_registry->view<procedural_tag>();
    auto resident_view = m_registry->view<island_worker_resident>();
    auto &node = m_registry->get<graph_node>(node_entity);
    auto &resident = m_registry->get<multi_island_worker_resident>(node_entity);

    // Add new non-procedural entity to islands of neighboring procedural entities.
    m_registry->ctx<entity_graph>().visit_neighbors(node.node_index, [&] (entt::entity other) {
        if (!procedural_view.contains(other)) return;

        auto &other_resident = resident_view.get<island_worker_resident>(other);
        if (other_resident.worker_entity == entt::null) return;

        auto &ctx = m_island_ctx_map.at(other_resident.worker_entity);
        ctx->m_nodes.insert(node_entity);

        if (!resident.worker_entities.count(other_resident.worker_entity)) {
            resident.worker_entities.insert(other_resident.worker_entity);
            ctx->m_delta_builder->created(node_entity);
            ctx->m_delta_builder->created_all(node_entity, *m_registry);
        }
    });
}

entt::entity island_coordinator::create_worker() {
    auto worker_entity = m_registry->create();
    auto &isle_time = m_registry->emplace<island_worker_timestamp>(worker_entity);
    isle_time.value = performance_time();

    auto [main_queue_input, main_queue_output] = make_message_queue_input_output();
    auto [isle_queue_input, isle_queue_output] = make_message_queue_input_output();

    // The `island_worker` is dynamically allocated and kept alive while
    // the associated island lives. The job that's created for it calls its
    // `update` function which reschedules itself to be run over and over again.
    // After the `finish` function is called on it (when the island is destroyed),
    // it will be deallocated on the next run.
    auto &settings = m_registry->ctx<edyn::settings>();
    auto &material_table = m_registry->ctx<edyn::material_mix_table>();
    auto *worker = new island_worker(settings, material_table,
                                     message_queue_in_out(main_queue_input, isle_queue_output));

    m_island_ctx_map[worker_entity] = std::make_unique<island_worker_context>(
        worker_entity, worker, (*settings.make_island_delta_builder)(),
        message_queue_in_out(isle_queue_input, main_queue_output));

    // Register to receive delta.
    auto &ctx = *m_island_ctx_map[worker_entity];
    ctx.island_delta_sink().connect<&island_coordinator::on_island_delta>(*this);

    return worker_entity;
}

void island_coordinator::batch_nodes(const std::vector<entt::entity> &nodes,
                                     const std::vector<entt::entity> &edges) {
    auto worker_entity = m_island_ctx_map.begin()->first;
    auto &ctx = *m_island_ctx_map[worker_entity];
    insert_to_worker(ctx, nodes, edges);
    ctx.send<island_delta>(ctx.m_delta_builder->finish());
}

void island_coordinator::insert_to_worker(entt::entity worker_entity,
                                          const std::vector<entt::entity> &nodes,
                                          const std::vector<entt::entity> &edges) {
    auto &ctx = m_island_ctx_map.at(worker_entity);
    insert_to_worker(*ctx, nodes, edges);
}

void island_coordinator::insert_to_worker(island_worker_context &ctx,
                                          const std::vector<entt::entity> &nodes,
                                          const std::vector<entt::entity> &edges) {

    ctx.m_nodes.insert(nodes.begin(), nodes.end());
    ctx.m_edges.insert(edges.begin(), edges.end());

    auto resident_view = m_registry->view<island_worker_resident>();
    auto multi_resident_view = m_registry->view<multi_island_worker_resident>();
    auto manifold_view = m_registry->view<contact_manifold>();
    auto procedural_view = m_registry->view<procedural_tag>();

    // Calculate total number of certain kinds of entities to later reserve
    // the expected number of components for better performance.
    size_t total_num_points = 0;
    size_t total_num_constraints = 0;

    for (auto entity : edges) {
        if (manifold_view.contains(entity)) {
            auto &manifold = manifold_view.get<contact_manifold>(entity);
            total_num_points += manifold.num_points();
            total_num_constraints += manifold.num_points();
        } else {
            total_num_constraints += 1;
        }
    }

    ctx.m_delta_builder->reserve_created(nodes.size() + edges.size() + total_num_points);
    ctx.m_delta_builder->reserve_created<contact_constraint>(total_num_points);
    ctx.m_delta_builder->reserve_created<contact_point>(total_num_points);
    ctx.m_delta_builder->reserve_created<constraint_impulse>(total_num_constraints);
    ctx.m_delta_builder->reserve_created<position, orientation, linvel, angvel, continuous>(nodes.size());
    ctx.m_delta_builder->reserve_created<mass, mass_inv, inertia, inertia_inv, inertia_world_inv>(nodes.size());
    auto worker_entity = ctx.worker_entity();

    for (auto entity : nodes) {
        if (procedural_view.contains(entity)) {
            auto &resident = resident_view.get<island_worker_resident>(entity);
            resident.worker_entity = worker_entity;
            ctx.m_delta_builder->created(entity);
            ctx.m_delta_builder->created_all(entity, *m_registry);
        } else {
            auto &resident = multi_resident_view.get<multi_island_worker_resident>(entity);

            if (resident.worker_entities.count(worker_entity) == 0) {
                // Non-procedural entity is not yet in this worker, thus create it.
                resident.worker_entities.insert(worker_entity);
                ctx.m_delta_builder->created(entity);
                ctx.m_delta_builder->created_all(entity, *m_registry);
            }
        }
    }

    for (auto entity : edges) {
        // Assign worker to residents. All edges are procedural, thus having an
        // `island_worker_resident`, which refers to a single worker.
        auto &resident = resident_view.get<island_worker_resident>(entity);
        resident.worker_entity = worker_entity;
        // Add new entities to the delta builder.
        ctx.m_delta_builder->created(entity);
        ctx.m_delta_builder->created_all(entity, *m_registry);

        // Add child entities.
        if (manifold_view.contains(entity)) {
            auto &manifold = manifold_view.get<contact_manifold>(entity);
            auto num_points = manifold.num_points();

            for (size_t i = 0; i < num_points; ++i) {
                auto point_entity = manifold.point[i];

                auto &point_resident = resident_view.get<island_worker_resident>(point_entity);
                point_resident.worker_entity = worker_entity;

                ctx.m_delta_builder->created(point_entity);
                ctx.m_delta_builder->created_all(point_entity, *m_registry);
            }
        }
    }
}

void island_coordinator::intersect_islands() {
    std::vector<entt::entity> awake_island_entities;

    // Update island AABBs in tree (ignore sleeping islands).
    auto exclude_sleeping = entt::exclude_t<sleeping_tag>{};
    m_registry->view<tree_resident, island_aabb>(exclude_sleeping)
        .each([&] (auto island_entity, tree_resident &node, island_aabb &aabb)
    {
        m_island_tree.move(node.id, aabb);
        awake_island_entities.push_back(island_entity);
    });

    if (awake_island_entities.empty()) {
        return;
    }

    // Update kinematic AABBs in tree.
    // TODO: only do this for kinematic entities that had their AABB updated.
    m_registry->view<tree_resident, AABB, kinematic_tag>()
        .each([&] (tree_resident &node, AABB &aabb)
    {
        m_np_tree.move(node.id, aabb);
    });

    // Search for island pairs with intersecting AABBs.
    const auto aabb_view = m_registry->view<AABB>();
    const auto island_aabb_view = m_registry->view<island_aabb>();
    const auto island_worker_resident_view = m_registry->view<island_worker_resident>();
    const auto multi_island_worker_resident_view = m_registry->view<multi_island_worker_resident>();

    if (awake_island_entities.size() > 1) {
        m_pair_results.resize(awake_island_entities.size());

        parallel_for(size_t{0}, awake_island_entities.size(), [&] (size_t index) {
            auto island_entityA = awake_island_entities[index];
            m_pair_results[index] = find_intersecting_islands(
                island_entityA, aabb_view, island_aabb_view,
                island_worker_resident_view, multi_island_worker_resident_view);
        });

        /* auto &manifold_map = m_registry->ctx<contact_manifold_map>();

        for (auto &results : m_pair_results) {
            for (auto &pair : results) {
                if (!manifold_map.contains(pair)) {
                    make_contact_manifold(*m_registry, pair.first, pair.second, m_separation_threshold);
                }
            }
        } */

        m_pair_results.clear();
    } else {
        for (auto island_entityA : awake_island_entities) {
            auto pairs = find_intersecting_islands(
                island_entityA, aabb_view, island_aabb_view,
                island_worker_resident_view, multi_island_worker_resident_view);

            for (auto &pair : pairs) {
                if (island_aabb_view.contains(pair.second)) {

                } else {
                    // Insert non-procedural node into the worker where the island
                    // is located.
                    EDYN_ASSERT((m_registry->any_of<static_tag, kinematic_tag>(pair.second)));
                    auto [worker_resident] =  island_worker_resident_view.get(pair.first);
                    auto [np_resident] = multi_island_worker_resident_view.get(pair.second);

                    if (!np_resident.worker_entities.count(worker_resident.worker_entity)) {
                        np_resident.worker_entities.insert(worker_resident.worker_entity);

                        auto &ctx = m_island_ctx_map.at(worker_resident.worker_entity);
                        ctx->m_delta_builder->created(pair.second);
                        ctx->m_delta_builder->created_all(pair.second, *m_registry);
                    }
                }
            }
        }
    }
}

entity_pair_vector island_coordinator::find_intersecting_islands(entt::entity island_entityA,
                                                              const aabb_view_t &aabb_view,
                                                              const island_aabb_view_t &island_aabb_view,
                                                              const island_worker_resident_view_t &island_worker_resident_view,
                                                              const multi_island_worker_resident_view_t &multi_island_worker_resident_view) const {
    auto island_aabbA = island_aabb_view.get<edyn::island_aabb>(island_entityA).inset(-m_island_aabb_offset);
    auto &residentA = island_worker_resident_view.get<island_worker_resident>(island_entityA);
    entity_pair_vector results;

    // Query the dynamic tree to find other islands whose AABB intersects the
    // current island's AABB.
    m_island_tree.query(island_aabbA, [&] (tree_node_id_t idB) {
        auto island_entityB = m_island_tree.get_node(idB).entity;

        if (island_entityA == island_entityB) {
            return;
        }

        // Only consider islands located in different workers.
        auto &residentB = island_worker_resident_view.get<island_worker_resident>(island_entityB);

        if (residentA.worker_entity != residentB.worker_entity) {
            results.emplace_back(island_entityA, island_entityB);
        }
    });

    // Query the non-procedural dynamic tree to find static and kinematic
    // entities that are intersecting the AABB of this island.
    m_np_tree.query(island_aabbA, [&] (tree_node_id_t id_np) {
        auto np_entity = m_np_tree.get_node(id_np).entity;

        // Only proceed if the non-procedural entity is not yet in the worker
        // where the island is located.
        auto &resident = multi_island_worker_resident_view.get<multi_island_worker_resident>(np_entity);

        if (!resident.worker_entities.count(residentA.worker_entity)) {
            results.emplace_back(island_entityA, np_entity);
        }
    });

    return results;
}

void island_coordinator::refresh_dirty_entities() {
    auto dirty_view = m_registry->view<dirty>();
    auto resident_view = m_registry->view<island_worker_resident>();
    auto multi_resident_view = m_registry->view<multi_island_worker_resident>();

    auto refresh = [this] (entt::entity entity, dirty &dirty, entt::entity worker_entity) {
        auto &ctx = m_island_ctx_map.at(worker_entity);
        auto &builder = ctx->m_delta_builder;

        if (dirty.is_new_entity) {
            builder->created(entity);
        }

        builder->created(entity, *m_registry,
            dirty.created_indexes.begin(), dirty.created_indexes.end());
        builder->updated(entity, *m_registry,
            dirty.updated_indexes.begin(), dirty.updated_indexes.end());
        builder->destroyed(entity,
            dirty.destroyed_indexes.begin(), dirty.destroyed_indexes.end());
    };

    dirty_view.each([&] (entt::entity entity, dirty &dirty) {
        if (resident_view.contains(entity)) {
            refresh(entity, dirty, resident_view.get<island_worker_resident>(entity).worker_entity);
        } else if (multi_resident_view.contains(entity)) {
            auto &resident = multi_resident_view.get<multi_island_worker_resident>(entity);
            for (auto worker_entity : resident.worker_entities) {
                refresh(entity, dirty, worker_entity);
            }
        }
    });

    m_registry->clear<dirty>();
}

void island_coordinator::on_island_delta(entt::entity source_worker_entity, const island_delta &delta) {
    m_importing_delta = true;
    auto &source_ctx = m_island_ctx_map.at(source_worker_entity);
    delta.import(*m_registry, source_ctx->m_entity_map);

    m_registry->get<island_worker_timestamp>(source_worker_entity).value = delta.m_timestamp;

    // Insert entity mappings for new entities into the current delta.
    for (auto remote_entity : delta.created_entities()) {
        if (!source_ctx->m_entity_map.has_rem(remote_entity)) continue;
        auto local_entity = source_ctx->m_entity_map.remloc(remote_entity);
        source_ctx->m_delta_builder->insert_entity_mapping(remote_entity, local_entity);
    }

    auto procedural_view = m_registry->view<procedural_tag>();
    auto node_view = m_registry->view<graph_node>();

    // Insert nodes in the graph for each rigid body.
    auto &graph = m_registry->ctx<entity_graph>();
    auto &index_source = source_ctx->m_delta_builder->get_index_source();
    auto insert_node = [&] (entt::entity remote_entity, auto &) {
        if (!source_ctx->m_entity_map.has_rem(remote_entity)) return;

        auto local_entity = source_ctx->m_entity_map.remloc(remote_entity);
        auto non_connecting = !m_registry->any_of<procedural_tag>(local_entity);
        auto node_index = graph.insert_node(local_entity, non_connecting);
        m_registry->emplace<graph_node>(local_entity, node_index);

        if (procedural_view.contains(local_entity)) {
            m_registry->emplace<island_worker_resident>(local_entity, source_worker_entity);
        } else {
            auto &resident = m_registry->emplace<multi_island_worker_resident>(local_entity);
            resident.worker_entities.insert(source_worker_entity);
        }

        source_ctx->m_nodes.insert(local_entity);
    };

    delta.created_for_each<dynamic_tag>(index_source, insert_node);
    delta.created_for_each<static_tag>(index_source, insert_node);
    delta.created_for_each<kinematic_tag>(index_source, insert_node);
    delta.created_for_each<external_tag>(index_source, insert_node);

    auto assign_island_to_contact_points = [&] (const contact_manifold &manifold) {
        auto num_points = manifold.num_points();

        for (size_t i = 0; i < num_points; ++i) {
            auto point_entity = manifold.point[i];

            if (m_registry->valid(point_entity) && !m_registry->any_of<island_worker_resident>(point_entity)) {
                m_registry->emplace<island_worker_resident>(point_entity, source_worker_entity);
            }
        }
    };

    delta.created_for_each<island>(index_source, [&] (entt::entity remote_entity, const island &) {
        if (!source_ctx->m_entity_map.has_rem(remote_entity)) return;

        auto local_entity = source_ctx->m_entity_map.remloc(remote_entity);
        m_registry->emplace<island_worker_resident>(local_entity, source_worker_entity);
    });

    // Insert edges in the graph for contact manifolds.
    delta.created_for_each<contact_manifold>(index_source, [&] (entt::entity remote_entity, const contact_manifold &manifold) {
        if (!source_ctx->m_entity_map.has_rem(remote_entity)) return;

        auto local_entity = source_ctx->m_entity_map.remloc(remote_entity);
        auto &node0 = node_view.get<graph_node>(manifold.body[0]);
        auto &node1 = node_view.get<graph_node>(manifold.body[1]);
        auto edge_index = graph.insert_edge(local_entity, node0.node_index, node1.node_index);
        m_registry->emplace<graph_edge>(local_entity, edge_index);
        m_registry->emplace<island_worker_resident>(local_entity, source_worker_entity);
        source_ctx->m_edges.insert(local_entity);

        assign_island_to_contact_points(manifold);
    });

    delta.updated_for_each<contact_manifold>(index_source, [&] (entt::entity, const contact_manifold &manifold) {
        assign_island_to_contact_points(manifold);
    });

    // Insert edges in the graph for constraints (except contact constraints).
    delta.created_for_each(constraints_tuple, index_source, [&] (entt::entity remote_entity, const auto &con) {
        // Contact constraints are not added as edges to the graph.
        // The contact manifold which owns them is added instead.
        if constexpr(std::is_same_v<std::decay_t<decltype(con)>, contact_constraint>) return;

        if (!source_ctx->m_entity_map.has_rem(remote_entity)) return;

        auto local_entity = source_ctx->m_entity_map.remloc(remote_entity);

        if (m_registry->any_of<graph_edge>(local_entity)) return;

        auto &node0 = node_view.get<graph_node>(con.body[0]);
        auto &node1 = node_view.get<graph_node>(con.body[1]);
        auto edge_index = graph.insert_edge(local_entity, node0.node_index, node1.node_index);
        m_registry->emplace<graph_edge>(local_entity, edge_index);
        m_registry->emplace<island_worker_resident>(local_entity, source_worker_entity);
        source_ctx->m_edges.insert(local_entity);
    });

    m_importing_delta = false;
}

void island_coordinator::sync() {
    for (auto &pair : m_island_ctx_map) {
        auto &ctx = pair.second;

        if (!ctx->delta_empty()) {
            ctx->send_delta();
        }

        ctx->flush();
    }
}

void island_coordinator::update() {
    m_timestamp = performance_time();

    for (auto &pair : m_island_ctx_map) {
        pair.second->read_messages();
    }

    init_new_nodes_and_edges();
    refresh_dirty_entities();
    sync();
    intersect_islands();
}

void island_coordinator::set_paused(bool paused) {
    for (auto &pair : m_island_ctx_map) {
        auto &ctx = pair.second;
        ctx->send<msg::set_paused>(paused);
    }
}

void island_coordinator::step_simulation() {
    for (auto &pair : m_island_ctx_map) {
        auto &ctx = pair.second;
        ctx->send<msg::step_simulation>();
    }
}

void island_coordinator::settings_changed() {
    auto &settings = m_registry->ctx<edyn::settings>();

    for (auto &pair : m_island_ctx_map) {
        auto &ctx = pair.second;
        ctx->send<msg::set_settings>(settings);
    }
}

void island_coordinator::material_table_changed() {
    auto &material_table = m_registry->ctx<material_mix_table>();

    for (auto &pair : m_island_ctx_map) {
        auto &ctx = pair.second;
        ctx->send<msg::set_material_table>(material_table);
    }
}

void island_coordinator::set_center_of_mass(entt::entity entity, const vector3 &com) {
    auto &resident = m_registry->get<island_worker_resident>(entity);
    auto &ctx = m_island_ctx_map.at(resident.worker_entity);
    ctx->send<msg::set_com>(entity, com);
}

}
