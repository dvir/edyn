#include "../common/common.hpp"

TEST(test_trimesh, voronoi_regions) {
    auto vertices = std::vector<edyn::vector3>{};
    vertices.push_back({1, 0, 1});
    vertices.push_back({1, 0, -1});
    vertices.push_back({-1, 0, -1});
    vertices.push_back({-1, 0, 1});
    vertices.push_back({0, 1, 0});
    vertices.push_back({2, 0, 0});

    auto indices = std::vector<uint32_t>{};
    indices.insert(indices.end(), {0, 1, 4});
    indices.insert(indices.end(), {1, 2, 4});
    indices.insert(indices.end(), {2, 3, 4});
    indices.insert(indices.end(), {3, 0, 4});
    indices.insert(indices.end(), {0, 5, 1});

    auto trimesh = edyn::triangle_mesh{};
    trimesh.insert_vertices(vertices.begin(), vertices.end());
    trimesh.insert_indices(indices.begin(), indices.end());
    trimesh.initialize();

    ASSERT_TRUE(trimesh.in_edge_voronoi(trimesh.get_face_edge_index(0, 1), {1, 1, -1}));
    ASSERT_TRUE(trimesh.in_edge_voronoi(trimesh.get_face_edge_index(0, 2), {1, 1, 1}));
    ASSERT_TRUE(trimesh.in_edge_voronoi(trimesh.get_face_edge_index(1, 1), {-1, 1, -1}));
    ASSERT_TRUE(trimesh.in_edge_voronoi(trimesh.get_face_edge_index(1, 2), {1, 1, -1}));
    ASSERT_TRUE(trimesh.in_edge_voronoi(trimesh.get_face_edge_index(2, 1), {-1, 1, 1}));
    ASSERT_FALSE(trimesh.in_edge_voronoi(trimesh.get_face_edge_index(2, 1), {1, -1, -1}));

    ASSERT_TRUE(trimesh.in_vertex_voronoi(4, {0, 1, 0}));
    ASSERT_TRUE(trimesh.in_vertex_voronoi(4, edyn::rotate(edyn::quaternion_axis_angle({0, 0, 1}, edyn::pi / 4 - EDYN_EPSILON), {0, 1, 0})));
    ASSERT_FALSE(trimesh.in_vertex_voronoi(4, {0, -1, 0}));
    ASSERT_FALSE(trimesh.in_vertex_voronoi(4, {1, 0, 0.5}));
    ASSERT_TRUE(trimesh.in_vertex_voronoi(2, {-1, 0.1, -1}));

    ASSERT_FALSE(trimesh.is_convex_edge(trimesh.get_face_edge_index(4, 2)));
}