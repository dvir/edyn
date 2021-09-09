#ifndef EDYN_COLLISION_BROADPHASE_MAIN_HPP
#define EDYN_COLLISION_BROADPHASE_MAIN_HPP

#include <map>
#include <vector>
#include <entt/entity/fwd.hpp>
#include <entt/entity/utility.hpp>
#include "edyn/comp/island.hpp"
#include "edyn/math/constants.hpp"
#include "edyn/collision/dynamic_tree.hpp"
#include "edyn/util/entity_pair.hpp"

namespace edyn {

class broadphase_main {

    using aabb_view_t = entt::basic_view<entt::entity, entt::exclude_t<>, AABB>;
    using island_aabb_view_t = entt::basic_view<entt::entity, entt::exclude_t<>, island_aabb>;
    using island_worker_resident_view_t = entt::basic_view<entt::entity, entt::exclude_t<>, island_worker_resident>;
    using multi_island_worker_resident_view_t = entt::basic_view<entt::entity, entt::exclude_t<>, multi_island_worker_resident>;

    // A higher threshold is used in the main broadphase to create contact
    // manifolds between different islands a little earlier and decrease the
    // probability they'll arrive in the corresponding island worker when the
    // shapes are already intersecting.
    constexpr static auto m_threshold = contact_breaking_threshold * scalar(4);
    constexpr static auto m_aabb_offset = vector3_one * -m_threshold;
    constexpr static auto m_separation_threshold = m_threshold * scalar(1.3);

    entity_pair_vector find_intersecting_islands(entt::entity island_entityA,
                                                 const aabb_view_t &,
                                                 const island_aabb_view_t &,
                                                 const island_worker_resident_view_t &,
                                                 const multi_island_worker_resident_view_t &) const;

public:
    broadphase_main(entt::registry &);
    void update();

    template<typename Func>
    void raycast_islands(vector3 p0, vector3 p1, Func func);

    template<typename Func>
    void raycast_non_procedural(vector3 p0, vector3 p1, Func func);

    void on_construct_island_aabb(entt::registry &, entt::entity);
    void on_construct_static_kinematic_tag(entt::registry &, entt::entity);
    void on_destroy_tree_resident(entt::registry &, entt::entity);

private:
    entt::registry *m_registry;
    dynamic_tree m_island_tree; // Tree for island AABBs.
    dynamic_tree m_np_tree; // Tree for non-procedural entities.
    std::vector<entity_pair_vector> m_pair_results;
};

template<typename Func>
void broadphase_main::raycast_islands(vector3 p0, vector3 p1, Func func) {
    m_island_tree.raycast(p0, p1, [&] (tree_node_id_t id) {
        func(m_island_tree.get_node(id).entity);
    });
}

template<typename Func>
void broadphase_main::raycast_non_procedural(vector3 p0, vector3 p1, Func func) {
    m_np_tree.raycast(p0, p1, [&] (tree_node_id_t id) {
        func(m_np_tree.get_node(id).entity);
    });
}

}

#endif // EDYN_COLLISION_BROADPHASE_MAIN_HPP
