#include "edyn/dynamics/restitution_solver.hpp"
#include "edyn/constraints/contact_constraint.hpp"
#include "edyn/constraints/constraint_row.hpp"
#include "edyn/constraints/constraint_impulse.hpp"
#include "edyn/collision/contact_manifold.hpp"
#include "edyn/collision/contact_point.hpp"
#include "edyn/util/constraint_util.hpp"
#include "edyn/comp/position.hpp"
#include "edyn/comp/orientation.hpp"
#include "edyn/comp/linvel.hpp"
#include "edyn/comp/angvel.hpp"
#include "edyn/comp/delta_linvel.hpp"
#include "edyn/comp/delta_angvel.hpp"
#include "edyn/comp/center_of_mass.hpp"
#include "edyn/comp/mass.hpp"
#include "edyn/comp/inertia.hpp"
#include "edyn/math/geom.hpp"
#include "edyn/dynamics/solver.hpp"
#include "edyn/parallel/entity_graph.hpp"
#include "edyn/comp/graph_node.hpp"
#include <entt/entity/registry.hpp>

namespace edyn {

bool solve_restitution_iteration(entt::registry &registry, scalar dt) {
    auto body_view = registry.view<position, orientation, linvel, angvel, mass_inv, inertia_world_inv, delta_linvel, delta_angvel>();
    auto cp_view = registry.view<contact_point>();
    auto imp_view = registry.view<constraint_impulse>();
    auto com_view = registry.view<center_of_mass>();
    auto restitution_view = registry.view<contact_manifold_with_restitution>();
    auto manifold_view = registry.view<contact_manifold>();

    const unsigned num_individual_restitution_iterations = 3;

    // Solve each manifold in isolation, prioritizing the ones with higher
    // relative velocity, since they're more likely to propagate velocity
    // to their neighbors.

    // Find manifold with highest penetration velocity.
    auto min_relvel = EDYN_SCALAR_MAX;
    auto fastest_manifold_entity = entt::entity{entt::null};

    for (auto entity : restitution_view) {
        auto &manifold = manifold_view.get(entity);
        auto num_points = manifold.num_points();

        if (num_points == 0) {
            continue;
        }

        auto [posA, ornA, linvelA, angvelA] =
            body_view.get<position, orientation, linvel, angvel>(manifold.body[0]);
        auto [posB, ornB, linvelB, angvelB] =
            body_view.get<position, orientation, linvel, angvel>(manifold.body[1]);

        auto originA = static_cast<vector3>(posA);
        auto originB = static_cast<vector3>(posB);

        if (com_view.contains(manifold.body[0])) {
            auto &com = com_view.get(manifold.body[0]);
            originA = to_world_space(-com, posA, ornA);
        }

        if (com_view.contains(manifold.body[1])) {
            auto &com = com_view.get(manifold.body[1]);
            originB = to_world_space(-com, posB, ornB);
        }

        auto local_min_relvel = EDYN_SCALAR_MAX;

        for (size_t pt_idx = 0; pt_idx < num_points; ++pt_idx) {
            auto &cp = cp_view.get(manifold.point[pt_idx]);
            auto normal = cp.normal;
            auto pivotA = to_world_space(cp.pivotA, originA, ornA);
            auto pivotB = to_world_space(cp.pivotB, originB, ornB);
            auto rA = pivotA - posA;
            auto rB = pivotB - posB;
            auto vA = linvelA + cross(angvelA, rA);
            auto vB = linvelB + cross(angvelB, rB);
            auto relvel = vA - vB;
            auto normal_relvel = dot(relvel, normal);
            local_min_relvel = std::min(normal_relvel, local_min_relvel);
        }

        if (local_min_relvel < min_relvel) {
            min_relvel = local_min_relvel;
            fastest_manifold_entity = entity;
        }
    }

    // This could be true if there are no contact points.
    if (fastest_manifold_entity == entt::null) {
        return true;
    }

    // In order to prevent bodies from bouncing forever, calculate a minimum
    // penetration velocity that must be met for the restitution impulse to
    // be applied.
    auto &fastest_manifold = manifold_view.get(fastest_manifold_entity);
    auto relvel_threshold = scalar(-0.005);

    if (min_relvel > relvel_threshold) {
        // All relative velocities are within threshold.
        return true;
    }

    auto normal_rows = std::vector<constraint_row>{};
    auto friction_row_pairs = std::vector<internal::contact_friction_row_pair>{};

    auto solveManifolds = [&] (const std::vector<entt::entity> &manifold_entities) {
        normal_rows.clear();
        friction_row_pairs.clear();

        for (auto manifold_entity : manifold_entities) {
            auto &manifold = manifold_view.get(manifold_entity);

            auto [posA, ornA, linvelA, angvelA, inv_mA, inv_IA, dvA, dwA] =
                body_view.get<position, orientation, linvel, angvel, mass_inv, inertia_world_inv, delta_linvel, delta_angvel>(manifold.body[0]);
            auto [posB, ornB, linvelB, angvelB, inv_mB, inv_IB, dvB, dwB] =
                body_view.get<position, orientation, linvel, angvel, mass_inv, inertia_world_inv, delta_linvel, delta_angvel>(manifold.body[1]);

            auto originA = static_cast<vector3>(posA);
            auto originB = static_cast<vector3>(posB);

            if (com_view.contains(manifold.body[0])) {
                auto &com = com_view.get(manifold.body[0]);
                originA = to_world_space(-com, posA, ornA);
            }

            if (com_view.contains(manifold.body[1])) {
                auto &com = com_view.get(manifold.body[1]);
                originB = to_world_space(-com, posB, ornB);
            }

            // Create constraint rows for non-penetration constraints for each
            // contact point.
            auto num_points = manifold.num_points();

            for (size_t pt_idx = 0; pt_idx < num_points; ++pt_idx) {
                auto point_entity = manifold.point[pt_idx];
                auto &cp = cp_view.get(point_entity);

                auto normal = cp.normal;
                auto pivotA = to_world_space(cp.pivotA, originA, ornA);
                auto pivotB = to_world_space(cp.pivotB, originB, ornB);
                auto rA = pivotA - posA;
                auto rB = pivotB - posB;

                auto &normal_row = normal_rows.emplace_back();
                normal_row.J = {normal, cross(rA, normal), -normal, -cross(rB, normal)};
                normal_row.inv_mA = inv_mA; normal_row.inv_IA = inv_IA;
                normal_row.inv_mB = inv_mB; normal_row.inv_IB = inv_IB;
                normal_row.dvA = &dvA; normal_row.dwA = &dwA;
                normal_row.dvB = &dvB; normal_row.dwB = &dwB;
                normal_row.lower_limit = 0;
                normal_row.upper_limit = large_scalar;

                auto normal_options = constraint_row_options{};
                normal_options.restitution = cp.restitution;

                prepare_row(normal_row, normal_options, linvelA, angvelA, linvelB, angvelB);

                auto &friction_row_pair = friction_row_pairs.emplace_back();
                friction_row_pair.friction_coefficient = cp.friction;

                vector3 tangents[2];
                plane_space(normal, tangents[0], tangents[1]);

                for (auto i = 0; i < 2; ++i) {
                    auto &friction_row = friction_row_pair.row[i];
                    friction_row.J = {tangents[i], cross(rA, tangents[i]), -tangents[i], -cross(rB, tangents[i])};
                    friction_row.eff_mass = get_effective_mass(friction_row.J, inv_mA, inv_IA, inv_mB, inv_IB);
                    friction_row.rhs = -get_relative_speed(friction_row.J, linvelA, angvelA, linvelB, angvelB);
                }
            }
        }

        // Solve rows.
        for (unsigned iter = 0; iter < num_individual_restitution_iterations; ++iter) {
            for (size_t pt_idx = 0; pt_idx < normal_rows.size(); ++pt_idx) {
                auto &normal_row = normal_rows[pt_idx];
                auto delta_impulse = solve(normal_row);
                apply_impulse(delta_impulse, normal_row);

                auto &friction_row_pair = friction_row_pairs[pt_idx];
                internal::solve_friction_row_pair(friction_row_pair, normal_row);
            }
        }

        // Persist applied impulses in a separate index because this cannot be
        // mixed with the regular constraint. It would apply these as the warm
        // starting impulse which will cause it to apply corrective impulses to
        // decelerate the rigid bodies which are separating.
        size_t row_idx = 0;

        for (auto manifold_entity : manifold_entities) {
            auto &manifold = manifold_view.get(manifold_entity);
            auto num_points = manifold.num_points();

            for (size_t pt_idx = 0; pt_idx < num_points; ++pt_idx) {
                auto &imp = imp_view.get(manifold.point[pt_idx]);
                auto &normal_row = normal_rows[row_idx];
                imp.values[3] = normal_row.impulse;

                auto &friction_row_pair = friction_row_pairs[row_idx];

                for (auto i = 0; i < 2; ++i) {
                    imp.values[4 + i] = friction_row_pair.row[i].impulse;
                }

                ++row_idx;
            }
        }

        // Apply delta velocities.
        for (auto manifold_entity : manifold_entities) {
            auto &manifold = manifold_view.get(manifold_entity);

            for (auto entity : manifold.body) {
                // There are duplicates among all manifold bodies but this
                // operation is idempotent since the delta velocity is set
                // to zero.
                auto [lv, av, dv, dw] = body_view.get<linvel, angvel, delta_linvel, delta_angvel>(entity);
                lv += dv;
                dv = vector3_zero;
                av += dw;
                dw = vector3_zero;
            }
        }
    };

    auto &graph = registry.ctx<entity_graph>();
    std::vector<entt::entity> manifold_entities;
    auto &start_node = registry.get<graph_node>(fastest_manifold.body[0]);

    graph.traverse_connecting_nodes(start_node.node_index, [&] (auto node_index) {
        graph.visit_edges(node_index, [&] (entt::entity edge_entity) {
            if (!manifold_view.contains(edge_entity)) return;

            auto &manifold = manifold_view.get(edge_entity);
            auto num_points = manifold.num_points();

            if (num_points == 0) {
                return;
            }

            auto [posA, ornA, linvelA, angvelA] =
                body_view.get<position, orientation, linvel, angvel>(manifold.body[0]);
            auto [posB, ornB, linvelB, angvelB] =
                body_view.get<position, orientation, linvel, angvel>(manifold.body[1]);

            auto originA = static_cast<vector3>(posA);
            auto originB = static_cast<vector3>(posB);

            if (com_view.contains(manifold.body[0])) {
                auto &com = com_view.get(manifold.body[0]);
                originA = to_world_space(-com, posA, ornA);
            }

            if (com_view.contains(manifold.body[1])) {
                auto &com = com_view.get(manifold.body[1]);
                originB = to_world_space(-com, posB, ornB);
            }

            // Ignore manifolds which are not penetrating fast enough.
            auto local_min_relvel = EDYN_SCALAR_MAX;

            for (size_t pt_idx = 0; pt_idx < num_points; ++pt_idx) {
                auto &cp = cp_view.get(manifold.point[pt_idx]);
                auto normal = cp.normal;
                auto pivotA = to_world_space(cp.pivotA, originA, ornA);
                auto pivotB = to_world_space(cp.pivotB, originB, ornB);
                auto rA = pivotA - posA;
                auto rB = pivotB - posB;
                auto vA = linvelA + cross(angvelA, rA);
                auto vB = linvelB + cross(angvelB, rB);
                auto relvel = vA - vB;
                auto normal_relvel = dot(relvel, normal);
                local_min_relvel = std::min(normal_relvel, local_min_relvel);
            }

            if (local_min_relvel < relvel_threshold) {
                manifold_entities.push_back(edge_entity);
            }
        });

        if (!manifold_entities.empty()) {
            solveManifolds(manifold_entities);
        }

        manifold_entities.clear();
    });

    return false;
}

void solve_restitution(entt::registry &registry, scalar dt) {
    const unsigned num_restitution_iterations = 10;

    for (unsigned i = 0; i < num_restitution_iterations; ++i) {
        if (solve_restitution_iteration(registry, dt)) {
            break;
        }
    }
}

}