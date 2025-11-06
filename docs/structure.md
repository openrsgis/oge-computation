# Structure of the project

There are three main parts of the project:

- oge-computation-commmon - contains common code for the computation, including config, entity, object storage tools.
- oge-computation-core - core implementation of the computation. 
- oge-computation-plugin - plugin for the computation.

## oge-computation-common
- config: Global config of the project.
- entity: Entity of the project.
- jsonparser: JSON parser of Trigger.
- objectStorage: Object storage tools.
- util: Utils of the project.
## oge-computation-core
- algorithms: Third party opern source algorithms.
  - SpatialStats: Spatial statistics algorithms.
  - terrain: Terrain algorithms.
- oge: OGE core implementation.
  - Coverage: Coverage algorithms, including coverage loading, processing, and exporting.
  - CoverageCollection: Coverage collection algorithms, including coverage collection loading, processing, and exporting.
  - Cube: Cube algorithms, including cube loading, processing, and exporting.
  - Feature: Feature algorithms, including feature loading, processing, and exporting.
  - Feature_deprecated: Deprecated feature algorithms.
  - FeatureCollection: Feature collection algorithms, including feature collection loading, processing, and exporting.
  - Filter: Filter algorithms, used to filter coverage in the coverage collection.
  - Geometry: Geometry algorithms, including geometry loading, processing, and exporting.
  - Kernel: Kernels for Coverage convolutions.
  - OGEGeometry: Geometry algorithms for OGE.
  - Service: Services for online deployment.
- trigger: Trigger for OGE online invocation. Some global variables are defined here.
- util: Utils of OGE core.