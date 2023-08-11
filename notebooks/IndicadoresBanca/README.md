# OOVS / INDICADORES BANCA
###### Indicadores de resultados requeridos por la banca en el marco del convenio de cofinanciacion de la Primera Linea del Metro de Bogotá.
------------
En el marco de la Linea de Credito Condicional para Proyectos de Inversion (CCLIP) para la Primera Linea del Metro de Bogota (PLMB), se establecieron los indicadores de resultados esperados de la operacion, cuyo objetivo es contribuir a mejorar la movilidad y la calidad de vida de la población de Bogotá a través de la PLMB, la cual aumentará la capacidad y mejorará la calidad de servicio del transporte y el aire, incluyendo una reducción de GEI, en el corredor del metro, así como la integración de la movilidad y **el desarrollo urbano mediante un patrón de ocupación del suelo eficiente, más intenso y diverso**, a lo largo del corredor de la PLMB.

Con respecto al desarrollo urbano, se tiene el resultado #5: Promover un uso mixto compacto y más denso en la zonificación del suelo, en donde se definieron los indicadores de Entropia (ENT) y Factor de Ocupacion Total (FOT), como se establece a continuacion:

## **ENTROPIA**
La ENT mide la mezcla del uso del suelo considerando el porcentaje relativo de dos o más tipos de uso del suelo dentro de un área específica, su valor máximo es 1 cuando existe un uso balanceado entre todos los usos, y su valor mínimo es 0 cuando el uso del suelo es homogéneo.

Para el caso del indicador (ENT) definido por la Banca, se calcula la entropia para dos usos; Residencial y Comercial. Este indicador se calcula a lo largo del corredor de la PLMB en una zona de influencia de 100 m a cada lado del corredor. Para este efecto, solo se tendran en cuenta los lotes considerados "desarrollables", es decir, aquellos que no tienen ninguna restricción urbanística para su desarrollo, como por ejemplo, condiciones patrimoniales, clasificación como equipamientos o usos institucionales.

<br><br>
<img src="https://acoustic-jump-94e.notion.site/image/https%3A%2F%2Fs3-us-west-2.amazonaws.com%2Fsecure.notion-static.com%2F37212ffe-33dc-4d83-9608-23b0cd7df5f7%2FUntitled.png?id=ac2f5614-1eae-4ed9-bdbf-10084017ac6b&table=block&spaceId=d93753ff-1ce2-49e0-b47a-fddd016cd6fa&width=2000&userId=&cache=v2" alt="OOVS" width="500">
<br>

### DATOS:

|  Dato | Descripcion  | Base   | Fuente  | Año  |
| :------------: | :------------: | :------------: | :------------: | :------------: |
|  Porcentaje del area total / uso | Porcentaje de representacion de cada uno de los usos (comercial y residencial) en el area de analisis.  | Censo inmobiliario  | UAECD  | 2006 a 2022  |
| BICs  | Identificacion de bienes de interes cultural a ser excluidos del calculo por no considerarse como predios desarrollables  | Base de datos geografica del Plan de Ordenamiento Territorial (Dec. 555 de 2021)  | SDP  | 2021  |
|  Predios requeridos para infraestructura PLMB | Identificacion de los predios requeridos para infraestructura de la PLMB para ser excluidos del calculo por no considerarse como predios desarrollables  | Estructuracion tecnica PLMB  | EMB  | 2021  |

### Area de Analisis
El area de analisis corresponde a una zona de influencia de 100 m a cada lado del corredor. Inicialmente se identifican las manzanas dentro de este buffer, posteriormente se categoriza cada manzana segun la estacion mas cercana, de manera tal que se genere un numero de tramos equivalente al numero de estaciones en la PLMB.

### Proceso de calculo
**1. Agrupacion a nivel de lote.** Los datos del censo inmobiliario identifican cada uno de los predios con su area y uso correspondiente. Inicialmente se agrupan todos los predios a nivel de lote segun su uso, de manera tal que se genere un dato unico de area para cada uso en un mismo lote.
**2. Reclasificacion de usos.** Los usos identificados en el censo inmobiliario indican un nivel de agregacion menor del requerido; existen varias categorias dentro del uso residencial y comercial, por lo que estas se reclasifican en estos dos usos unicamente, usando la *tabla de referencia de usos* del OOVS. Los demas usos se eliminan ya que no son requeridos para el calculo del indicador.
**3. Exclusion de predios no desarrollables.** Se realiza la proyeccion espacial de los lotes identificados y se cruzan con los predios requeridos para la PLMB y los Bienes de Interes Cultural. se eliminan de la base principal aquellos predios que coinciden con los  predios requeridos para la PLMB y los Bienes de Interes Cultural.
**4. Calculo de factores.** se calculan los porcentajes de area total para cada uso (Residencial y Comercial) en cada uno de los tramos identificados en el area de analisis.
**5. Calculo del indicador.** Se aplica la formula para cada una de las estaciones y años disponibles para el calculo, en resumen se saca la tabla pivot por año.

## Resultados iteracion 1
<br>
<img src="https://acoustic-jump-94e.notion.site/image/https%3A%2F%2Fs3-us-west-2.amazonaws.com%2Fsecure.notion-static.com%2F473af09b-a29b-499e-ad22-10f8826b9d68%2FUntitled.png?id=65533bab-eda6-494a-9e7a-feb1cefb70d7&table=block&spaceId=d93753ff-1ce2-49e0-b47a-fddd016cd6fa&width=2000&userId=&cache=v2" alt="OOVS" width="800">

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
<br>
<img src="https://acoustic-jump-94e.notion.site/image/https%3A%2F%2Fs3-us-west-2.amazonaws.com%2Fsecure.notion-static.com%2F6f3c6e61-adb1-4574-8929-947e6402611a%2FUntitled.png?id=8a255c8d-5cf1-4943-a89c-5028b3a6ecd5&table=block&spaceId=d93753ff-1ce2-49e0-b47a-fddd016cd6fa&width=770&userId=&cache=v2" alt="OOVS" width="300">
