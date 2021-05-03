#ifndef EDYN_UTIL_ENTT_UTIL_HPP
#define EDYN_UTIL_ENTT_UTIL_HPP

#include <tuple>
#include <entt/fwd.hpp>
#include <entt/entity/utility.hpp>
#include <entt/entity/registry.hpp>

namespace edyn {

// Generate a view type using a tuple of component types.
template<typename... Ts>
struct tuple_view_type;

template<typename... Ts>
struct tuple_view_type<std::tuple<Ts...>> {
    using type = entt::basic_view<entt::entity, entt::exclude_t<>, Ts...>;
};

// Get a view with component types from a tuple.
template<typename... Ts>
auto get_tuple_view(entt::registry &registry, std::tuple<Ts...>) {
    return registry.view<Ts...>();
}

}

#endif // EDYN_UTIL_ENTT_UTIL_HPP
