#ifndef EDYN_PARALLEL_ISLAND_COORDINATOR_HPP
#define EDYN_PARALLEL_ISLAND_COORDINATOR_HPP

#include <vector>
#include <memory>
#include <unordered_map>
#include <entt/entity/fwd.hpp>
#include "edyn/math/scalar.hpp"
#include "edyn/comp/island.hpp"
#include "edyn/parallel/island_delta.hpp"
#include "edyn/parallel/island_worker_context.hpp"
#include "edyn/parallel/island_delta_builder.hpp"
#include "edyn/parallel/message.hpp"

namespace edyn {

class island_worker;
class island_delta;

/**
 * Manages all simulation islands. Creates and destroys island workers as necessary
 * and synchronizes the workers and the main registry.
 */
class island_coordinator final {

    void init_new_nodes_and_edges();
    void init_new_non_procedural_node(entt::entity);
    entt::entity create_worker();
    void insert_to_worker(island_worker_context &ctx,
                          const std::vector<entt::entity> &nodes,
                          const std::vector<entt::entity> &edges);
    void insert_to_worker(entt::entity worker_entity,
                          const std::vector<entt::entity> &nodes,
                          const std::vector<entt::entity> &edges);
    void refresh_dirty_entities();
    void sync();

public:
    island_coordinator(island_coordinator const&) = delete;
    island_coordinator operator=(island_coordinator const&) = delete;
    island_coordinator(entt::registry &);
    ~island_coordinator();

    void on_construct_graph_node(entt::registry &, entt::entity);
    void on_construct_graph_edge(entt::registry &, entt::entity);

    void on_destroy_graph_node(entt::registry &, entt::entity);
    void on_destroy_graph_edge(entt::registry &, entt::entity);

    void on_destroy_island_worker_resident(entt::registry &, entt::entity);
    void on_destroy_multi_island_worker_resident(entt::registry &, entt::entity);
    void on_island_delta(entt::entity, const island_delta &);

    void update();

    void set_paused(bool);
    void step_simulation();

    template<typename... Component>
    void refresh(entt::entity entity);

    void set_center_of_mass(entt::entity entity, const vector3 &com);

    // Call when settings have changed in the registry's context. It will
    // propagate changes to island workers.
    void settings_changed();

    void material_table_changed();

    void batch_nodes(const std::vector<entt::entity> &nodes,
                     const std::vector<entt::entity> &edges);

private:
    entt::registry *m_registry;
    std::unordered_map<entt::entity, std::unique_ptr<island_worker_context>> m_island_ctx_map;

    std::vector<entt::entity> m_new_graph_nodes;
    std::vector<entt::entity> m_new_graph_edges;

    bool m_importing_delta {false};
    double m_timestamp;
};

template<typename... Component>
void island_coordinator::refresh(entt::entity entity) {
    static_assert(sizeof...(Component) > 0);

    if (m_registry->any_of<island_worker_resident>(entity)) {
        auto &resident = m_registry->get<island_worker_resident>(entity);
        auto &ctx = m_island_ctx_map.at(resident.worker_entity);
        ctx->m_delta_builder->updated<Component...>(entity, *m_registry);
    } else {
        auto &resident = m_registry->get<multi_island_worker_resident>(entity);
        for (auto worker_entity : resident.worker_entities) {
            auto &ctx = m_island_ctx_map.at(worker_entity);
            ctx->m_delta_builder->updated<Component...>(entity, *m_registry);
        }
    }
}

}

#endif // EDYN_PARALLEL_ISLAND_COORDINATOR_HPP
