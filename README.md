# OGE Computation

This is an open-source implementation of the Oep Geospatial Engine ([OGE](https://www.openge.org.cn/en-US)) Computation framework.
OGE Computation is an open-source geospatial computation framework designed to process and analyze raster data cubes. 
It supports operations such as addition, subtraction, multiplication, and division on raster tiles, as well as integration with geospatial libraries for efficient data handling.

## Features

- **Data Cube Operations**: Perform mathematical operations (add, subtract, multiply, divide) on data cubes.
- **Geospatial Data Handling**: Supports reprojecting, tiling, and writing raster data.
- **Scalable Distributed Processing**: Built on Apache Spark for distributed data processing.

---

## Dependencies

This project uses the following dependencies:

- **Scala**: Primary programming language for the project.
- **Apache Spark**: Distributed data processing framework.


Refer to the `pom.xml` file for the complete list of dependencies.

---

## Configuration

1. **Java and Scala Setup**:
    - Ensure you have Java 8 and Scala 2.12.10 installed on your system.
    - Set the `JAVA_HOME` environment variable to point to your Java installation.

2. **Maven Configuration**:
    - Install Maven (3.6+ recommended).
    - Verify Maven installation using `mvn -v`.

3. **Project Structure**:
    - The source code is located in `src/main/scala`.
    - The project uses a parent Maven module (`oge-computation-ogc-parent`) for dependency management.

4. **Build the Project**:
    - Run the following command to build the project:
      ```bash
      mvn clean install
      ```

---

## Quick Start

Follow these steps to get started with the project:

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd oge-computation
   ```
2. **Build the Project**:

   3. **Run the Application**:
      Run the application in a main function:
      ```scala
      import whu.edu.cn.oge.Coverage._
      
      def main(args: Array[String]): Unit = {
         val sc = new SparkContext("local[*]", "Coverage")
         val filePath = "localFilePath"
         val coverage = loadFromLocalTiff(sc, filePath)
         val coverage1 = slope(coverage,1,1)
         makeTIFF(coverage1,"result","outPutFilePath")
      }
      ```

4. **Add New Raster Operations**:
   - Modify the `Coverage.scala` file to add your own functions.
   - Rebuild and rerun the project to see the results.


