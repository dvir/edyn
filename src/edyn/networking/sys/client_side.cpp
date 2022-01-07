#include "edyn/networking/sys/client_side.hpp"
#include "edyn/collision/contact_manifold.hpp"
#include "edyn/config/config.h"
#include "edyn/edyn.hpp"
#include "edyn/networking/comp/entity_owner.hpp"
#include "edyn/parallel/entity_graph.hpp"
#include "edyn/comp/graph_edge.hpp"
#include "edyn/comp/graph_node.hpp"
#include "edyn/constraints/soft_distance_constraint.hpp"
#include "edyn/networking/comp/networked_comp.hpp"
#include "edyn/networking/packet/entity_request.hpp"
#include "edyn/networking/packet/util/pool_snapshot.hpp"
#include "edyn/networking/context/client_networking_context.hpp"
#include "edyn/comp/dirty.hpp"
#include "edyn/comp/tag.hpp"
#include "edyn/parallel/job_dispatcher.hpp"
#include "edyn/parallel/merge/merge_component.hpp"
#include "edyn/networking/extrapolation_job.hpp"
#include "edyn/networking/util/client_import_pool.hpp"
#include "edyn/time/time.hpp"
#include "edyn/util/tuple_util.hpp"
#include <entt/entity/registry.hpp>
#include <set>

namespace edyn {

void on_construct_networked_entity(entt::registry &registry, entt::entity entity) {
    auto &ctx = registry.ctx<client_networking_context>();

    if (!ctx.importing_entities) {
        ctx.created_entities.push_back(entity);
    }
}

void on_destroy_networked_entity(entt::registry &registry, entt::entity entity) {
    auto &ctx = registry.ctx<client_networking_context>();

    if (!ctx.importing_entities) {
        ctx.destroyed_entities.push_back(entity);

        if (ctx.entity_map.has_loc(entity)) {
            ctx.entity_map.erase_loc(entity);
        }
    }
}

void on_construct_entity_owner(entt::registry &registry, entt::entity entity) {
    auto &ctx = registry.ctx<client_networking_context>();
    auto &owner = registry.get<entity_owner>(entity);

    if (owner.client_entity == ctx.client_entity) {
        ctx.owned_entities.emplace(entity);
    }
}

void on_destroy_entity_owner(entt::registry &registry, entt::entity entity) {
    auto &ctx = registry.ctx<client_networking_context>();
    auto &owner = registry.get<entity_owner>(entity);

    if (owner.client_entity == ctx.client_entity) {
        ctx.owned_entities.erase(entity);
    }
}

void init_networking_client(entt::registry &registry) {
    registry.set<client_networking_context>();

    registry.on_construct<networked_tag>().connect<&on_construct_networked_entity>();
    registry.on_destroy<networked_tag>().connect<&on_destroy_networked_entity>();
    registry.on_construct<entity_owner>().connect<&on_construct_entity_owner>();
    registry.on_destroy<entity_owner>().connect<&on_destroy_entity_owner>();
}

void deinit_networking_client(entt::registry &registry) {
    registry.unset<client_networking_context>();

    registry.on_construct<networked_tag>().disconnect<&on_construct_networked_entity>();
    registry.on_destroy<networked_tag>().disconnect<&on_destroy_networked_entity>();
    registry.on_construct<entity_owner>().disconnect<&on_construct_entity_owner>();
    registry.on_destroy<entity_owner>().disconnect<&on_destroy_entity_owner>();
}

void update_networking_client(entt::registry &registry) {
    auto &ctx = registry.ctx<client_networking_context>();

    if (!ctx.created_entities.empty()) {
        packet::create_entity packet;
        packet.entities = ctx.created_entities;

        for (auto entity : ctx.created_entities) {
            (*ctx.insert_entity_components_func)(registry, entity, packet.pools);
        }

        std::sort(packet.pools.begin(), packet.pools.end(), [] (auto &&lhs, auto &&rhs) {
            return lhs.component_index < rhs.component_index;
        });

        ctx.packet_signal.publish(packet::edyn_packet{std::move(packet)});
        ctx.created_entities.clear();
    }

    if (!ctx.destroyed_entities.empty()) {
        packet::destroy_entity packet;
        packet.entities = std::move(ctx.destroyed_entities);
        ctx.packet_signal.publish(packet::edyn_packet{std::move(packet)});
    }

    auto time = performance_time();

    if (time - ctx.last_snapshot_time > 1 / ctx.snapshot_rate) {
        auto packet = packet::transient_snapshot{};

        for (auto entity : ctx.owned_entities) {
            EDYN_ASSERT(registry.all_of<networked_tag>(entity));
            (*ctx.insert_transient_components_func)(registry, entity, packet.pools);
        }

        ctx.last_snapshot_time = time;

        if (!packet.pools.empty()) {
            ctx.packet_signal.publish(packet::edyn_packet{std::move(packet)});
        }
    }

    // Check if extrapolation jobs are finished and merge their results into
    // the main registry.
    for (auto &extr_ctx : ctx.extrapolation_jobs) {
        if (!extr_ctx.job->is_finished()) continue;


        extr_ctx.m_message_queue.update();

        // TODO
    }
}

static void process_packet(entt::registry &registry, const packet::client_created &packet) {
    auto &ctx = registry.ctx<client_networking_context>();
    ctx.importing_entities = true;

    auto remote_entity = packet.client_entity;
    auto local_entity = registry.create();
    edyn::tag_external_entity(registry, local_entity, false);

    EDYN_ASSERT(ctx.client_entity == entt::null);
    ctx.client_entity = local_entity;
    ctx.client_entity_assigned_signal.publish();
    ctx.entity_map.insert(remote_entity, local_entity);

    auto emap_packet = packet::update_entity_map{};
    emap_packet.pairs.emplace_back(remote_entity, local_entity);
    ctx.packet_signal.publish(packet::edyn_packet{std::move(emap_packet)});

    ctx.importing_entities = false;
}

static void process_packet(entt::registry &registry, const packet::update_entity_map &emap) {
    auto &ctx = registry.ctx<client_networking_context>();

    for (auto &pair : emap.pairs) {
        auto local_entity = pair.first;
        auto remote_entity = pair.second;
        ctx.entity_map.insert(remote_entity, local_entity);
    }
}

static void process_packet(entt::registry &registry, const packet::entity_request &req) {

}

static void process_packet(entt::registry &registry, const packet::entity_response &res) {
    auto &ctx = registry.ctx<client_networking_context>();
    ctx.importing_entities = true;

    auto emap_packet = packet::update_entity_map{};

    for (auto remote_entity : res.entities) {

        if (ctx.entity_map.has_rem(remote_entity)) {
            continue;
        }

        auto local_entity = registry.create();
        ctx.entity_map.insert(remote_entity, local_entity);
        emap_packet.pairs.emplace_back(remote_entity, local_entity);
    }

    for (auto &pool : res.pools) {
        (*ctx.import_pool_func)(registry, pool);
    }

    for (auto remote_entity : res.entities) {
        auto local_entity = ctx.entity_map.remloc(remote_entity);
        registry.emplace<networked_tag>(local_entity);
    }

    ctx.importing_entities = false;

    if (!emap_packet.pairs.empty()) {
        ctx.packet_signal.publish(packet::edyn_packet{std::move(emap_packet)});
    }
}

template<typename T>
void create_graph_edge(entt::registry &registry, entt::entity entity) {
    if (registry.any_of<graph_edge>(entity)) return;

    auto &comp = registry.get<T>(entity);
    auto node_index0 = registry.get<graph_node>(comp.body[0]).node_index;
    auto node_index1 = registry.get<graph_node>(comp.body[1]).node_index;
    auto edge_index = registry.ctx<entity_graph>().insert_edge(entity, node_index0, node_index1);
    registry.emplace<graph_edge>(entity, edge_index);
}

template<typename... Ts>
void maybe_create_graph_edge(entt::registry &registry, entt::entity entity) {
    ((registry.any_of<Ts>(entity) ? create_graph_edge<Ts>(registry, entity) : void(0)), ...);
}

static void process_packet(entt::registry &registry, const packet::create_entity &packet) {
    auto &ctx = registry.ctx<client_networking_context>();
    ctx.importing_entities = true;

    // Collect new entity mappings to send back to server.
    auto emap_packet = packet::update_entity_map{};

    // Create entities first...
    for (auto remote_entity : packet.entities) {
        if (ctx.entity_map.has_rem(remote_entity)) continue;
        auto local_entity = registry.create();
        ctx.entity_map.insert(remote_entity, local_entity);
        emap_packet.pairs.emplace_back(remote_entity, local_entity);
    }

    if (!emap_packet.pairs.empty()) {
        ctx.packet_signal.publish(packet::edyn_packet{std::move(emap_packet)});
    }

    // ... assign components later so that entity references will be available
    // to be mapped into the local registry.
    for (auto &pool : packet.pools) {
        (*ctx.import_pool_func)(registry, pool);
    }

    for (auto remote_entity : packet.entities) {
        auto local_entity = ctx.entity_map.remloc(remote_entity);
        registry.emplace_or_replace<networked_tag>(local_entity);
    }

    // Create nodes and edges in entity graph.
    for (auto remote_entity : packet.entities) {
        auto local_entity = ctx.entity_map.remloc(remote_entity);

        if (registry.any_of<rigidbody_tag, external_tag>(local_entity) &&
            !registry.all_of<graph_node>(local_entity)) {
            auto non_connecting = !registry.any_of<procedural_tag>(local_entity);
            auto node_index = registry.ctx<entity_graph>().insert_node(local_entity, non_connecting);
            registry.emplace<graph_node>(local_entity, node_index);
        }
    }

    for (auto remote_entity : packet.entities) {
        auto local_entity = ctx.entity_map.remloc(remote_entity);

        maybe_create_graph_edge<
            contact_manifold,
            null_constraint,
            gravity_constraint,
            point_constraint,
            distance_constraint,
            soft_distance_constraint,
            hinge_constraint,
            generic_constraint
        >(registry, local_entity);
    }

    ctx.importing_entities = false;
}

static void process_packet(entt::registry &registry, const packet::destroy_entity &packet) {
    auto &ctx = registry.ctx<client_networking_context>();
    ctx.importing_entities = true;

    for (auto remote_entity : packet.entities) {
        if (!ctx.entity_map.has_rem(remote_entity)) continue;

        auto local_entity = ctx.entity_map.remloc(remote_entity);
        ctx.entity_map.erase_rem(remote_entity);

        if (registry.valid(local_entity)) {
            registry.destroy(local_entity);
        }
    }

    ctx.importing_entities = false;
}

static void process_packet(entt::registry &registry, const packet::transient_snapshot &snapshot) {
    auto &ctx = registry.ctx<client_networking_context>();
    double snapshot_time = performance_time() - (ctx.server_playout_delay + ctx.round_trip_time / 2);

    // Collect the node indices of all entities involved in the snapshot.
    std::set<entity_graph::index_type> node_indices;
    auto node_view = registry.view<graph_node>();

    for (auto &pool : snapshot.pools) {
        auto pool_entities = pool.ptr->get_entities();

        for (auto entity : pool_entities) {
            if (node_view.contains(entity)) {
                auto node_index = node_view.get<graph_node>(entity).node_index;
                node_indices.insert(node_index);
            }
        }
    }

    // Traverse entity graph to find all entities connected to the entities
    // present in the transient snapshot.
    auto entities = entt::sparse_set{};
    auto &graph = registry.ctx<entity_graph>();
    graph.reach(
        node_indices.begin(), node_indices.end(),
        [&] (entt::entity entity) { // visitNodeFunc
            if (!entities.contains(entity)) {
                entities.emplace(entity);
            }
        },
        [&] (entt::entity entity) { // visitEdgeFunc
            if (!entities.contains(entity)) {
                entities.emplace(entity);
            }
        },
        [&] (entity_graph::index_type) { // shouldVisitFunc
            return true;
        },
        [] () { // connectedComponentFunc
        });

    // Create registry snapshot to send to extrapolation job.
    auto builder = make_island_delta_builder(registry);
    for (auto entity : entities) {
        builder->created(entity);
        builder->created_all(entity, registry);
    }

    // Create extrapolation job and put the registry snapshot and the transient
    // snapshot into its message queue.
    auto &settings = registry.ctx<edyn::settings>();
    auto &material_table = registry.ctx<material_mix_table>();

    auto [main_queue_input, main_queue_output] = make_message_queue_input_output();
    auto [isle_queue_input, isle_queue_output] = make_message_queue_input_output();

    auto job = std::make_unique<extrapolation_job>(snapshot_time, settings, material_table, ctx.import_pool_func,
                                                   message_queue_in_out(main_queue_input, isle_queue_output));
    main_queue_input.send<island_delta>(builder->finish());
    main_queue_input.send<packet::transient_snapshot>(snapshot);

    job->reschedule();

    ctx.extrapolation_jobs.push_back(extrapolation_job_context{std::move(job),
                                     message_queue_in_out(isle_queue_input, main_queue_output)});
}

static void process_packet(entt::registry &registry, const packet::general_snapshot &snapshot) {
    auto &ctx = registry.ctx<client_networking_context>();

    for (auto &pool : snapshot.pools) {
        (*ctx.import_pool_func)(registry, pool);
    }
}

static void process_packet(entt::registry &registry, const packet::set_playout_delay &delay) {
    auto &ctx = registry.ctx<client_networking_context>();
    ctx.server_playout_delay = delay.value;
}

void client_handle_packet(entt::registry &registry, const packet::edyn_packet &packet) {
    std::visit([&] (auto &&inner_packet) {
        process_packet(registry, inner_packet);
    }, packet.var);
}

entt::sink<void()> on_client_entity_assigned(entt::registry &registry) {
    auto &ctx = registry.ctx<client_networking_context>();
    return entt::sink{ctx.client_entity_assigned_signal};
}

bool client_owns_entity(entt::registry &registry, entt::entity entity) {
    auto &ctx = registry.ctx<client_networking_context>();
    return ctx.client_entity == registry.get<entity_owner>(entity).client_entity;
}

}