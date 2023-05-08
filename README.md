# EjercicioTriciclos
En este repositorio se encuentran programas de Python con la librería pyspark para hallar los triciclos de un grafo.

SOBRE LOS ARCHIVOS:
- triciclosV1.py es un programa de Python escrito con la librería pyspark que toma como entrada un grafo mediante un fichero txt como los que se adjuntan (g1.txt o g2.txt)
e imprime por pantalla los triciclos que tiene el grafo. Es importante saber que al imprimir por pantalla el resultado, se debe hacer un collect() y si el grafo es muy grande puede
causar problemas. El hecho de imprimirlo por pantalla es para facilitar ver el funcionamiento del programa con grafos pequeños.

- triciclosV2.py es igual que triciclosV1.py pero ahora el grafo puede estar escrito en varios ficheros que se toman como entrada.

- triciclosV3.py es igual que triciclosV1.py pero ahora toma como entrada varios grafos, cada uno en un archivo de texto, e imprime por pantalla los triciclos de cada grafo por separado.

- g1.txt y g2.txt son ejemplos de grafos que toman como entrada los programas anteriores.
