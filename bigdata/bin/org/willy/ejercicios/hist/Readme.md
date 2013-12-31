Histograma 
=======

En este ejercicio tenemos que calcular los histogramas, dados un conjunto de entrada,
dividido en diferentes elementos por línea.

## Funcionamiento
 -  Calcular el mínimo y el máximo en un primer MR.
 -  Calcular dónde va cada elemento a partir de máximo y el mínino en otro MR.

## Máximo y mínimo

### Driver

Especialmente se configura el path de salida, del cual se leerá luego en el cálculo de las barras.
La salida del fichero es como sigue:

``` 
<mínimo>
<máximo>
``` 

Además, se configura como Combiner el mismo que el Reducer, para rebajar la transferencia de datos
que se realiza por la red.

### Mapper

Simplemente se emite un par `(null, valor)`

### Reducer

Por cada clave (que sólo hay una: `null`), se comprueba si es el posible que sea el máximo y 
el mínimo.

Al final se emite el mínimo y luego el máximo en el fichero, que luego será leído por el cálculo de barras.

## Cálculo de barras

### Driver

Se configura, teniendo en cuenta que hay 3 entradas por línea de comandos:
 - `input`: lo mismo que en el anterior
 - `output`: simplemente el output
 - `<número barras>`: en cuántas barras tenemos que separar los datos de entrada
 
Además, es necesario leer el fichero que sacó el MR anterior para ver el mínimo y el máximo.
Simplemente lo parseo y lo paso en la configuración del Job, que luego permite que se lea desde
diferentes mappers o reducers.

### Mapper

Se calcula la barra, sabiendo que puede haber problemas en los valores máximos y mínimos, porque
están fuera de los intervalos más extremos, por lo que se tratan de forma especial.

Se emite la barra como clave y el valor es nulo.

### Reducer

Agrupados por barra (usando un Writable personalizado), se cuentan el número de elementos
para cada barra y se emite al final la suma.

## Main Runner

En esta clase, está el  `main` del ejercicio, que ejecuta los dos tools secuencialmente.