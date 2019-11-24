#ifndef EDYN_UTIL_RIGIDBODY_HPP
#define EDYN_UTIL_RIGIDBODY_HPP

#include <optional>
#include <entt/fwd.hpp>
#include "edyn/math/vector3.hpp"
#include "edyn/math/quaternion.hpp"
#include "edyn/comp/shape.hpp"

namespace edyn {

enum class rigidbody_kind : uint8_t {
    // A rigid body with non-zero and finite mass that reacts to forces and
    // impulses and can be affected by constraints.
    rb_dynamic,

    // A rigid body that is not affected by others and can be moved directly.
    rb_kinematic,

    // A rigid body that is not affected by others and never changes.
    rb_static
};

struct rigidbody_def {
    // The entity kind will determine which components are added to it
    // in the `make_rigidbody` call.
    rigidbody_kind kind {rigidbody_kind::rb_dynamic};

    // Initial position and orientation.
    vector3 position {vector3_zero};
    quaternion orientation {quaternion_identity};

    // Mass properties for dynamic entities.
    scalar mass {1};
    vector3 inertia {1, 1, 1};

    // Initial linear and angular velocity.
    vector3 linvel {vector3_zero};
    vector3 angvel {vector3_zero};

    // Optional shape for collidable entities.
    std::optional<decltype(shape::var)> shape_opt; 

    scalar restitution {0.8};
    scalar friction {0.5};

    bool sensor {false};

    // Whether this entity will be used for presentation and needs 
    // position/orientation interpolation.
    bool presentation {false};
};

void make_rigidbody(entt::entity, entt::registry &, const rigidbody_def &);
entt::entity make_rigidbody(entt::registry &, const rigidbody_def &);

void update_kinematic_position(entt::registry &, entt::entity, const vector3 & pos, scalar dt);

}

#endif // EDYN_UTIL_RIGIDBODY_HPP