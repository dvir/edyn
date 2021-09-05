from conans import ConanFile, CMake, tools


class EdynConan(ConanFile):
    name = "edyn"
    version = "0.2.0"
    license = "MIT"
    author = "xissburg <xissburg@xissburg.com>"
    url = "https://github.com/xissburg/edyn"
    description = "Edyn is a real-time physics engine organized as an ECS. "
    topics = ("game-development", "physics-engine", "ecs",
              "entity-component-system")
    settings = "cppstd", "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = {"shared": False, "fPIC": True}
    generators = "cmake", "cmake_find_package", "cmake_paths"
    exports = ["LICENSE"]
    exports_sources = "src/**"
    requires = "entt/3.8.0"

    def config_options(self):
        self.settings.compiler.cppstd = "17"
        if self.settings.os == "Windows":
            del self.options.fPIC

    def source(self):
        self.run("git clone https://github.com/xissburg/edyn.git")

    def build(self):
        cmake = CMake(self)
        cmake.configure(source_folder="edyn")
        cmake.build()

        # Explicit way:
        # self.run('cmake %s/hello %s'
        #          % (self.source_folder, cmake.command_line))
        # self.run("cmake --build . %s" % cmake.build_config)

    def package(self):
        self.copy(pattern="LICENSE", dst="licenses")
        for pattern in ("*.h", "*.hpp"):
            self.copy(pattern, dst="include", src="include")
            self.copy(pattern, dst="include", src="edyn/include")
        for pattern in ("*.a", "*.so", "*.dylib", "*.dll"):
            self.copy(pattern, dst="lib", keep_path=False)

    def package_info(self):
        if self.settings.os == "Windows":
            self.cpp_info.libs = ["winmm"]
        elif self.settings.os == "Linux" or self.settings.os == "Macos":
            self.cpp_info.libs = ["pthread", "dl"]
        self.cpp_info.libs.append("Edyn")
        self.cpp_info.names["cmake_find_package"] = "Edyn"
        self.cpp_info.names["cmake_find_package_multi"] = "Edyn"

    def validate(self):
        tools.check_min_cppstd(self, "17")
