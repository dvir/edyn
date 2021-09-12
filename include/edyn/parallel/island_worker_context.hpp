#ifndef EDYN_PARALLEL_ISLAND_WORKER_CONTEXT_HPP
#define EDYN_PARALLEL_ISLAND_WORKER_CONTEXT_HPP

#include <memory>
#include <entt/signal/fwd.hpp>
#include "edyn/parallel/island_delta.hpp"
#include "edyn/util/entity_set.hpp"
#include "edyn/util/entity_map.hpp"
#include "edyn/parallel/message.hpp"
#include "edyn/parallel/island_worker.hpp"
#include "edyn/parallel/message_dispatcher.hpp"

namespace edyn {

class island_delta_builder;

/**
 * Context of an island worker in the main thread in an island coordinator.
 */
class island_worker_context {

    island_worker *m_worker;
    bool m_pending_flush;

public:
    entity_set m_nodes;
    entity_set m_edges;
    entity_set m_islands;
    entity_map m_entity_map;
    std::unique_ptr<island_delta_builder> m_delta_builder;
    double m_timestamp;

    island_worker_context(island_worker *worker, std::unique_ptr<island_delta_builder> delta_builder);

    /**
     * Returns whether the current delta doesn't contain any changes.
     */
    bool delta_empty() const;

    /**
     * Returns whether the island needs to be waken up after sending the
     * current delta to it.
     */
    bool delta_needs_wakeup() const;

    /**
     * Sends current registry delta and clears it up, making it ready for more
     * updates.
     */
    void send_delta(message_queue_identifier source);

    /**
     * Ensures messages are delivered and processed by waking up the worker
     * in case it is sleeping.
     */
    void flush();

    template<typename Message, typename... Args>
    void send(message_queue_identifier source, Args &&... args) {
        message_dispatcher::global().send<Message>(m_worker->message_queue_id(), source, std::forward<Args>(args)...);
        m_pending_flush = true;
    }

    /**
     * Schedules worker to be terminated.
     */
    void terminate();
};

}

#endif // EDYN_PARALLEL_ISLAND_WORKER_CONTEXT_HPP
