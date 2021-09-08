#ifndef EDYN_COMP_ISLAND_HPP
#define EDYN_COMP_ISLAND_HPP

#include "edyn/comp/aabb.hpp"
#include "edyn/util/entity_set.hpp"
#include <entt/entity/fwd.hpp>
#include <entt/entity/entity.hpp>
#include <optional>

namespace edyn {

/**
 * @brief An _island_ is a set of entities that can affect one another,
 * usually through constraints.
 */
struct island {
    entity_set nodes;
    entity_set edges;
    std::optional<double> sleep_timestamp;
};

struct island_aabb : public AABB {};

/**
 * @brief Component assigned to an entity that resides in an island, i.e.
 * procedural entities which can only be present in a single island.
 */
struct island_resident {
    entt::entity island_entity {entt::null};
};

struct island_worker_timestamp {
    double value;
};

struct island_worker_resident {
    entt::entity worker_entity {entt::null};
};

/**
 * @brief Component assigned to an entity that resides in multiple islands,
 * i.e. non-procedural entities which can be present in multiple islands
 * simultaneously.
 */
struct multi_island_worker_resident {
    entity_set worker_entities;
};

}

#endif // EDYN_COMP_ISLAND_HPP
