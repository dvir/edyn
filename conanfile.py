from conans import ConanFile, CMake, tools


class EdynConan(ConanFile):
    name = "edyn"
    version = "0.1.0"
    license = "MIT"
    author = "xissburg <xissburg@xissburg.com>"
    url = "https://github.com/xissburg/edyn"
    description = "Edyn is a real-time physics engine organized as an ECS. "
    topics = ("game-development", "physics-engine", "ecs", "entity-component-system")
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = {"shared": False, "fPIC": True}
    generators = "cmake", "cmake_find_package", "cmake_paths"
    exports = ["LICENSE"]
    exports_sources = "src/**"
    requires = "entt/3.8.0"

    def config_options(self):
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
        self.copy("edyn", dst="include", src="include", keep_path=True)

    def package_info(self):
        self.cpp_info.libs = ["edyn"]
