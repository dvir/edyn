#include "edyn/sys/update_aabbs.hpp"
#include "edyn/comp/orientation.hpp"
#include "edyn/comp/position.hpp"
#include "edyn/comp/shape.hpp"
#include "edyn/comp/aabb.hpp"
#include "edyn/comp/tag.hpp"
#include "edyn/util/aabb_util.hpp"
#include <entt/entity/registry.hpp>

namespace edyn {

template<typename ShapeType>
AABB updated_aabb(const ShapeType &shape, const vector3 &pos, const quaternion &orn) {
    return shape_aabb(shape, pos, orn);
}

template<>
AABB updated_aabb(const polyhedron_shape &polyhedron,
                  const vector3 &pos, const quaternion &orn) {
    // `shape_aabb` rotates each vertex of a polyhedron to calculate the AABB.
    // Specialize `updated_aabb` for polyhedrons to use the rotated mesh.
    return point_cloud_aabb(polyhedron.rotated->vertices);
}

template<typename ShapeType>
void update_aabbs(entt::registry &registry) {
    auto view = registry.view<position, orientation, ShapeType, AABB>();
    view.each([] (position &pos, orientation &orn, ShapeType &shape, AABB &aabb) {
        aabb = updated_aabb(shape, pos, orn);
    });
}

template<typename... Ts>
void update_aabbs(entt::registry &registry, std::tuple<Ts...>) {
    (update_aabbs<Ts>(registry), ...);
}

void update_aabbs(entt::registry &registry) {
    update_aabbs(registry, non_static_shapes_tuple_t{});
}

}