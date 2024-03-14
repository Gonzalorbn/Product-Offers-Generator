# script para consultar api de mercado libre y guardar la información relevante en un archivo tsv
import requests
import json

def get_most_relevant_items_for_category(category):
    """
    Recibimos los items mas relevantes por categoria.La variable category
    se le deberá asignar la categoría que queramos usar.
    La variable response termina transformandose en un json

    """
    url = (f'https://api.mercadolibre.com/sites/MLA/search?category={category}#json')
    response = requests.get(url).text
    response = json.loads(response)
    data = response["results"]
    datos_filtrados = []
    for item in data:
        # Crear un nuevo diccionario solo con las claves deseadas
        item_filtrado = {
            'title': getKeyFromItem(item, 'title'),
            'price': getKeyFromItem(item, 'price'),
            'original_price': getKeyFromItem(item, 'original_price'),
            'available_quantity': getKeyFromItem(item, 'available_quantity'),
            'permalink': getKeyFromItem(item, 'permalink'),
            'thumbnail': getKeyFromItem(item, 'thumbnail')
        }
        datos_filtrados.append(item_filtrado)

    return datos_filtrados
            
def getKeyFromItem(item, key):
    """
    Recibo la key que necesito sacar del diccionario almacenado en item
    """
    return str(item[key]).replace('','').strip() if item.get(key) else "null"

def main(category):

    resultados_filtrados = get_most_relevant_items_for_category(category)

    with open('D:/Facultad/cursos/CursoPDE/Airflow-ej-tecnico/plugins/tmp/ofertas.tsv', 'w', encoding='utf-8') as file:
        for item_filtrado in resultados_filtrados:                          
                file.write(f"{item_filtrado.get('title', 'null')}\t"
                           f"{item_filtrado.get('price', 'null')}\t"
                           f"{item_filtrado.get('original_price', 'null')}\t"
                           f"{item_filtrado.get('available_quantity', 'null')}\t"
                           f"{item_filtrado.get('permalink', 'null')}\t"
                           f"{item_filtrado.get('thumbnail', 'null')}\n")
if __name__ == "__main__":                                         
    main("MLA1055")
