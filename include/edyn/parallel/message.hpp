#ifndef EDYN_PARALLEL_MESSAGE_HPP
#define EDYN_PARALLEL_MESSAGE_HPP

#include "edyn/math/scalar.hpp"
#include "edyn/context/settings.hpp"
#include "edyn/dynamics/material_mixing.hpp"
#include "edyn/parallel/island_delta.hpp"
#include "edyn/parallel/message_dispatcher.hpp"

namespace edyn::msg {

struct set_paused {
    bool paused;
};

struct set_settings {
    edyn::settings settings;
};

struct set_material_table {
    edyn::material_mix_table table;
};

struct step_simulation {};

struct set_com {
    entt::entity entity;
    vector3 com;
};

/**
 * Message sent by worker to coordinator after every step of the simulation
 * containing everything that changed since the previous update.
 */
struct step_update {
    island_delta delta;
};

/**
 * Message sent by the coordinator to a worker asking entities and components
 * to be created, destroyed or updated.
 */
struct update_entities {
    island_delta delta;
};

/**
 * A request to transfer an island from one worker to another.
 */
struct transfer_island_request {
    entt::entity island_entity;
    message_queue_identifier destination_id;
};

struct transfer_island {
    island_delta delta;
    entt::entity island_entity;
};

/**
 * Acknowledgement that an island has been transferred into a new worker
 * successfully.
 */
struct island_transfer_complete {
    // All entities transferred.
    std::vector<entt::entity> entities;
    edyn::entity_map entity_map;
};

/**
 * Response sent back when an island transfer is not performed, perhaps because
 * the island doesn't exist anymore or it's temporarily locked.
 */
struct island_transfer_failure {
    entt::entity island_entity;
};

}

#endif // EDYN_PARALLEL_MESSAGE_HPP
