#include "edyn/collision/collide.hpp"
#include "edyn/collision/collision_result.hpp"
#include "edyn/math/constants.hpp"
#include "edyn/math/quaternion.hpp"
#include "edyn/math/scalar.hpp"

namespace edyn {

void collide(const polyhedron_shape &shA, const plane_shape &shB, 
             const collision_context &ctx, collision_result &result) {
    auto &posA = ctx.posA;
    auto &rmeshA = *shA.rotated;

    auto normal = shB.normal;
    auto center = shB.normal * shB.constant;
    scalar distance = EDYN_SCALAR_MAX;

    // Find distance between polyhedron and plane.
    for (auto &rvertex : rmeshA.vertices) {
        auto vertex_world = posA + rvertex;
        auto vertex_dist = dot(vertex_world - center, normal);
        distance = std::min(vertex_dist, distance);
    }

    if (distance > ctx.threshold) return;

    // Add points to all vertices that are within a range from the
    // minimum distance.
    for (size_t i = 0; i < rmeshA.vertices.size(); ++i) {
        auto vertex_world = posA + rmeshA.vertices[i];
        auto vertex_dist = dot(vertex_world - center, normal);

        if (vertex_dist > distance + support_feature_tolerance) continue;

        auto pivotA = shA.mesh->vertices[i];
        auto pivotB = vertex_world - normal * vertex_dist;
        result.maybe_add_point({pivotA, pivotB, normal, vertex_dist});
    }
}

void collide(const plane_shape &shA, const polyhedron_shape &shB,
             const collision_context &ctx, collision_result &result) {
    swap_collide(shA, shB, ctx, result);
}

}
