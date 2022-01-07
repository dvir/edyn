#ifndef EDYN_NETWORKING_AABB_OF_INTEREST_HPP
#define EDYN_NETWORKING_AABB_OF_INTEREST_HPP

#include "edyn/comp/aabb.hpp"
#include <entt/signal/sigh.hpp>
#include <entt/entity/sparse_set.hpp>
#include <vector>

namespace edyn {

struct aabb_of_interest {
    // The AABB of interest.
    AABB aabb {vector3_one * -500, vector3_one * 500};

    // Entities which are part of an island whose AABB intersected the AABB of
    // interest in the last update.
    entt::sparse_set entities;

    // Entities that entered and exited the AABB in the last update. These
    // containers are for temporary data storage in the AABB of interest update
    // and so they get cleared up in every update and should not be modified.
    std::vector<entt::entity> create_entities;
    std::vector<entt::entity> destroy_entities;
};

}

#endif // EDYN_NETWORKING_AABB_OF_INTEREST_HPP