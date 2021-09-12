#include "edyn/parallel/island_worker_context.hpp"
#include "edyn/parallel/island_delta.hpp"
#include "edyn/parallel/island_worker.hpp"
#include "edyn/parallel/island_delta_builder.hpp"

namespace edyn {

island_worker_context::island_worker_context(island_worker *worker,
                                             std::unique_ptr<island_delta_builder> delta_builder)
    : m_worker(worker)
    , m_delta_builder(std::move(delta_builder))
    , m_pending_flush(false)
{
}

bool island_worker_context::delta_empty() const {
    return m_delta_builder->empty();
}

bool island_worker_context::delta_needs_wakeup() const {
    return m_delta_builder->needs_wakeup();
}

void island_worker_context::send_delta(message_queue_identifier source) {
    send<msg::update_entities>(source, m_delta_builder->finish());
}

void island_worker_context::flush() {
    if (m_pending_flush) {
        m_worker->reschedule();
        m_pending_flush = false;
    }
}

void island_worker_context::terminate() {
    m_worker->terminate();
}

}
