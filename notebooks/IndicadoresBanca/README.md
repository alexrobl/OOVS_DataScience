# Oovs Medidas Banca

# **ENTROPIA**

La ENT, o entropía, se calcula a lo largo del corredor de la PLMB en una zona de influencia de 100 m a cada lado del corredor.

La ENT mide la mezcla del uso del suelo considerando el porcentaje relativo de dos o más tipos de uso del suelo dentro de un área específica, y su valor máximo es 1 cuando existe un uso balanceado entre todos los usos, y su valor mínimo es 0 cuando el uso del suelo es homogéneo.

Este índice se calculará en cada zona de influencia en lotes considerados "desarrollables", es decir, aquellos que no tienen ninguna restricción urbanística para su desarrollo, como por ejemplo, condiciones patrimoniales, clasificación como equipamientos o usos institucionales.

<img src="https://acoustic-jump-94e.notion.site/image/https%3A%2F%2Fs3-us-west-2.amazonaws.com%2Fsecure.notion-static.com%2F37212ffe-33dc-4d83-9608-23b0cd7df5f7%2FUntitled.png?id=ac2f5614-1eae-4ed9-bdbf-10084017ac6b&table=block&spaceId=d93753ff-1ce2-49e0-b47a-fddd016cd6fa&width=2000&userId=&cache=v2" alt="OOVS" width="500">

# Fuentes de DATOS:

- Catastro censo inmobiliario (2006-2022). Registros administrativos
- Planeación
    - Predios patrimonio
    - BICS
    - Equipamientos
- EMB
    - Predios Adquiridos para infraestructura y estaciones.

# Area de Estudio

https://www.google.com/maps/d/u/1/edit?mid=1XH_UHl0pTo5LKN2mV52QkqaxhfvtDgqu&usp=sharing

### Proceso de calculo

1. Agrupación de **inmuebles por lote** y tipo de uso, Reducción de características (Atributos). Catastro censo inmobiliario (2006-2022). Registros administrativos.
2. Reclasificación de usos. 
3. En la linea del viaducto se aplica la herramienta de area de influencia para calcular el buffer a 100 mts. 
4. Cruce entre los datos agrupados y las capas de planeación, estas seran nuestras restricciones para el calculo.
5. Para facilitar el calculo e interpretación. Se reproyectan las estaciones (Coordenadas planas) y se le asignan la manzana más cercana. Algoritmo “sjoin_nearest”.


<img src="https://acoustic-jump-94e.notion.site/image/https%3A%2F%2Fs3-us-west-2.amazonaws.com%2Fsecure.notion-static.com%2F6ddf24b4-d891-416f-a60a-e4a86863c4e6%2FUntitled.png?id=331da8be-5406-4475-8e78-b0dc8648ad1d&table=block&spaceId=d93753ff-1ce2-49e0-b47a-fddd016cd6fa&width=960&userId=&cache=v2" alt="OOVS" width="400">

    
6. Se aplica la formula para cada una de las estaciones y años disponibles para el calculo, en resumen se saca la tabla pivot por año. 

## Resultados iteracion 1

<img src="https://acoustic-jump-94e.notion.site/image/https%3A%2F%2Fs3-us-west-2.amazonaws.com%2Fsecure.notion-static.com%2F473af09b-a29b-499e-ad22-10f8826b9d68%2FUntitled.png?id=65533bab-eda6-494a-9e7a-feb1cefb70d7&table=block&spaceId=d93753ff-1ce2-49e0-b47a-fddd016cd6fa&width=2000&userId=&cache=v2" alt="OOVS" width="500">

# FACTOR DE OCUPACIÓN TOTAL

Es un **Índice de Construcción** Total que se realiza sobre la totalidad de lotes considerados desarrollables (estos se refieren a los lotes que no tengan ninguna restricción urbanística para su desarrollo, como condición patrimonial, clasificación como equipamientos de escala metropolitana, urbana o zonal, espacio público y/o lotes consolidados con una altura mayor a 3 pisos e índice de construcción superior a 3) y con la totalidad del área construida para cada uno de los lotes.

# Fuentes de DATOS:

- Catastro censo inmobiliario (2006-2022). Registros administrativos
- Planeación
    - Predios patrimonio
    - BICS
    - Equipamientos
    - Numero pisos
- EMB
    - Predios Adquiridos para infraestructura y estaciones.

### Proceso de calculo

1. MANZANAS A 200 MTS AlREDEDOR DE LAS ESTACIONES.
2. IDENTIFICAR LOS BIENES DE INTERES CULTURAL
3. IDENTIFICAR LOS SECTORES DE INTERES URBANISTICO (PEMPS)
4. FILTRAR LOS USOS DIFERENTES A EQUIPAMIENTOS O USOS INSTITUCIONALES
5. FILTRAR NUMERO DE PISOS MENORES O IGUALES A 3 PISOS
6. DE LA INFORMACION DE CATASTRO DE PH, SACAR AQUELLOS LOTES CON CONSTRUCCIONES MAYORES A 3 PISOS
7. QUITAR LOS PREDIOS QUE SERAN DEMOLIDOS.

# Estructura Resultados FOT:
<img src="https://acoustic-jump-94e.notion.site/image/https%3A%2F%2Fs3-us-west-2.amazonaws.com%2Fsecure.notion-static.com%2F6f3c6e61-adb1-4574-8929-947e6402611a%2FUntitled.png?id=8a255c8d-5cf1-4943-a89c-5028b3a6ecd5&table=block&spaceId=d93753ff-1ce2-49e0-b47a-fddd016cd6fa&width=770&userId=&cache=v2" alt="OOVS" width="500">