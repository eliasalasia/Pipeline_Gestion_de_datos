# Implementación de un Pipeline para la Gestión de Datos en Mercadona

## Flujo ETL
Este proyecto implementa un flujo ETL (Extracción, Transformación y Carga) automatizado utilizando Apache Airflow, contenedores Docker y una base de datos PostgreSQL.

## El proceso consiste en:
### Extracción:
Los datos de inventario y ventas se obtienen a partir de archivos CSV locales ubicados en la carpeta /opt/airflow/data.
### Transformación:
A través de DAGs de Airflow, los datos son procesados usando pandas:
- Se verifica la consistencia entre productos.
- Se calcula el stock actualizado combinando inventario y ventas.
### Carga:
Los datos transformados se cargan en tablas PostgreSQL:
- inventario
- ventas
- reporte_stock (resultado final del flujo, con stock actualizado por producto y almacén)

*(Actualmente los DAGs estan programados para ejecución manual desde la interfaz de Airflow, para que esten completamente automatizados hay que agregarles el schedule_interval en cada DAG :)*

---

## Estructura del Proyecto
```
pipeline_mercadona/
├── dags/
| ├── dag_inventario.py
| ├── dag_ventas.py
| └── dag_reporte.py
├── data/
| ├── inventario.csv
| ├── ventas.csv
├── imagenes/
| ├── airflow.png 
│ ├── inventario.png
│ ├── reporte_stock.png
│ ├── ventas.png
├── README.md 
├── docker-compose.yml
```
---

## Tecnologías Utilizadas
- Linux
- Apache Airflow
- PostgreSQL
- Docker y Docker Compose
- Python 3
- DBeaver (para visualizacion de datos)

---

## Cómo levantar el entorno

1. Clona el repositorio:
   ```bash
   git clone https://github.com/tuusuario/airflow_mercadona.git
   cd airflow_mercadona
   ```
2. Levantar los contenedores con docker-compose:
  ```bash
   sudo docker-compose up -d
  ```
3. Acceder a la interfaz web de Airflow:
   http://localhost:8081
    Usuario: admin
    Contraseña: admin

## Funcionamiento de los DAGs
- dag_inventario.py
  Carga los datos del archivo inventario.csv a la tabla inventario.

- dag_ventas.py
  Carga los datos del archivo ventas.csv a la tabla ventas.

- dag_reporte.py
  Genera la tabla reporte_stock combinando inventario y ventas para calcular el stock final.
## Esquema de la Base de Datos
```sql
CREATE TABLE reporte_stock (
    producto VARCHAR(100),
    almacen VARCHAR(100),
    cantidad_original INTEGER,
    unidades_vendidas INTEGER,
    stock_actual INTEGER
);
CREATE TABLE ventas (
    id SERIAL PRIMARY KEY,
    producto VARCHAR(255) NOT NULL,
    unidades INT NOT NULL,
    fecha DATE NOT NULL,
    CONSTRAINT fk_producto FOREIGN KEY (producto) REFERENCES inventario(producto)
);
CREATE TABLE inventario (
    id SERIAL PRIMARY KEY,
    producto VARCHAR(255) NOT NULL,
    cantidad INT NOT NULL,
    almacen VARCHAR(255) NOT NULL,
    CONSTRAINT unique_producto UNIQUE (producto)
);
```
