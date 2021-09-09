#include "edyn/collision/broadphase_main.hpp"
#include "edyn/collision/tree_node.hpp"
#include "edyn/comp/aabb.hpp"
#include "edyn/comp/island.hpp"
#include "edyn/comp/tree_resident.hpp"
#include "edyn/comp/tag.hpp"
#include "edyn/parallel/parallel_for.hpp"
#include "edyn/context/settings.hpp"
#include <entt/entity/registry.hpp>

namespace edyn {

broadphase_main::broadphase_main(entt::registry &registry)
    : m_registry(&registry)
{
    // Add tree nodes for islands and for static and kinematic entities when
    // they're created.
    registry.on_construct<island_aabb>().connect<&broadphase_main::on_construct_island_aabb>(*this);
    registry.on_construct<static_tag>().connect<&broadphase_main::on_construct_static_kinematic_tag>(*this);
    registry.on_construct<kinematic_tag>().connect<&broadphase_main::on_construct_static_kinematic_tag>(*this);
    registry.on_destroy<tree_resident>().connect<&broadphase_main::on_destroy_tree_resident>(*this);
}

void broadphase_main::on_construct_island_aabb(entt::registry &registry, entt::entity entity) {
    auto &aabb = registry.get<island_aabb>(entity);
    auto id = m_island_tree.create(aabb, entity);
    registry.emplace<tree_resident>(entity, id, true);
}

void broadphase_main::on_construct_static_kinematic_tag(entt::registry &registry, entt::entity entity) {
    if (!registry.any_of<AABB>(entity)) return;

    auto &aabb = registry.get<AABB>(entity);
    auto id = m_np_tree.create(aabb, entity);
    registry.emplace<tree_resident>(entity, id, false);
}

void broadphase_main::on_destroy_tree_resident(entt::registry &registry, entt::entity entity) {
    auto &node = registry.get<tree_resident>(entity);

    if (node.procedural) {
        m_island_tree.destroy(node.id);
    } else {
        m_np_tree.destroy(node.id);
    }
}

void broadphase_main::update() {
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

            /* for (auto &pair : pairs) {
                make_contact_manifold(*m_registry, pair.first, pair.second, m_separation_threshold);
            } */
        }
    }
}

entity_pair_vector broadphase_main::find_intersecting_islands(entt::entity island_entityA,
                                                              const aabb_view_t &aabb_view,
                                                              const island_aabb_view_t &island_aabb_view,
                                                              const island_worker_resident_view_t &island_worker_resident_view,
                                                              const multi_island_worker_resident_view_t &multi_island_worker_resident_view) const {
    auto island_aabbA = island_aabb_view.get<edyn::island_aabb>(island_entityA).inset(m_aabb_offset);
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

}
