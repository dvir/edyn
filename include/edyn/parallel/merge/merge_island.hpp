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

}

#endif // EDYN_PARALLEL_MERGE_MERGE_ISLAND_RESIDENT_HPP
