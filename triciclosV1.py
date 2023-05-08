from pyspark import SparkContext
import sys

'''
Programa que toma como entrada el nombre de fichero que tiene el grafo e imprime por
pantalla todos los triciclos
'''

'''
Función que dado un str de la forma A,B o "A","B" devuelve la tupla (A,B)
'''
def arista(line):
    arista = line.strip().split(',')
    v1 = arista[0].strip('"')
    v2 = arista[1].strip('"')
    if v1 < v2:
         return (v1,v2)
    elif v1 > v2:
         return (v2,v1)
    else:
        return None

'''
Toma como entrada una tupla de la forma (A, [B,C])
y devuelve una lista indicando las aristas que existen
y los posibles triciclos que se podrán formar:
    [((A,B), exists), ((A,C), exists), ((B,C), ('pending', A))]
'''
def posibles_triciclos(nodo_adyacencia):
    result = []
    num_adyacentes = len(nodo_adyacencia[1])
    for i in range(num_adyacentes):
        result.append(((nodo_adyacencia[0],nodo_adyacencia[1][i]),'exists'))
        for j in range(i+1, num_adyacentes):
            if nodo_adyacencia[1][i] < nodo_adyacencia[1][j]:
                result.append(((nodo_adyacencia[1][i],nodo_adyacencia[1][j]),('pending',nodo_adyacencia[0])))
            else:
                result.append(((nodo_adyacencia[1][j],nodo_adyacencia[1][i]),('pending',nodo_adyacencia[0])))
    return result


'''
Toma como entrada una arista (clave) y una lista cuyos elementos son 'exists' o tuplas
de la forma ('pending', A). De tal forma que si una arista tiene en la lista 'exists' es porque
efectivamente existe y si tiene ('pending', A) es porque estaba pendiente la existencia de la
arista en cuestión para formar triciclo con el vértice A. Así, si tiene ambas cosas implica que
la arista forma parte de un triciclo.
'''
def es_triciclo(arista):
    return 'exists' in arista[1] and len(arista[1])>1 #Nótese que para saber si era una arista pendiente y existe, solo basta saber si la lista tiene más de un elemento ya que exists solo aparece una vez


'''
Toma como entrada una arista (clave) y una lista cuyos elementos son 'exists' o tuplas
de la forma ('pending', A). Devuelve la lista de triciclos en forma de tupla.
'''
def dame_triciclos_por_arista(arista):
    result = []
    for x in arista[1]:
        if x != 'exists':
            result.append((x[1], arista[0][0], arista[0][1]))
    return result
    
def main(sc, grafo_fich):
    grafo_txt = sc.textFile(grafo_fich)
    grafo = grafo_txt.map(arista).filter(lambda x: x is not None).distinct() #Ahora tenemos una lista de tuplas sin aristas duplicadas
    adyacencia = grafo.groupByKey().mapValues(list) #Ahora cada vértice (clave) tiene asociado la lista de vértices mayores que él adyacentes (valor)
    posibles_triciclos_por_nodo = adyacencia.flatMap(posibles_triciclos)
    posibles_triciclos_por_arista = posibles_triciclos_por_nodo.groupByKey().mapValues(list)
    aristas_en_triciclo = posibles_triciclos_por_arista.filter(es_triciclo)
    triciclos = aristas_en_triciclo.flatMap(dame_triciclos_por_arista)
    print(list(triciclos.collect()))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, sys.argv[1])
