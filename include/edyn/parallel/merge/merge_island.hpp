#ifndef EDYN_PARALLEL_MERGE_MERGE_ISLAND_RESIDENT_HPP
#define EDYN_PARALLEL_MERGE_MERGE_ISLAND_RESIDENT_HPP

#include "edyn/comp/island.hpp"
#include "edyn/parallel/merge/merge_component.hpp"
#include "edyn/util/entity_map.hpp"

namespace edyn {

template<> inline
void merge(const island_resident *old_comp, island_resident &new_comp, merge_context &ctx) {
    if (new_comp.island_entity != entt::null) {
        new_comp.island_entity = ctx.map->remloc(new_comp.island_entity);
    }
}

template<> inline
void merge(const island *old_comp, island &new_comp, merge_context &ctx) {
    auto nodes = new_comp.nodes;
    auto edges = new_comp.edges;

    new_comp.nodes = {};
    new_comp.edges = {};

    for (auto entity : nodes) {
        new_comp.nodes.insert(ctx.map->remloc(entity));
    }

    for (auto entity : edges) {
        new_comp.edges.insert(ctx.map->remloc(entity));
    }
}

}

#endif // EDYN_PARALLEL_MERGE_MERGE_ISLAND_RESIDENT_HPP
