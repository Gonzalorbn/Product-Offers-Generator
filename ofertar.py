#script de transformación de datos, donde extraigo solo los datos relevantes para el analisis
from email.message import EmailMessage
import ssl
import smtplib
from airflow.models import Variable

def analizar_y_ofertar(datos):

    items_con_descuento = []

    for item_filtrado in datos:
        title = item_filtrado.get('title', 'null')
        price = item_filtrado.get('price', 'null')
        original_price = item_filtrado.get('original_price', 'null')
        available_quantity = item_filtrado.get('available_quantity', 'null')
        thumbnail = item_filtrado.get('thumbnail', 'null')
        permalink = item_filtrado.get('permalink', 'null')

       # Realiza la comparación para verificar si hay un descuento del 20% o más y si está disponible
        if original_price != 'null' and price != 'null' and available_quantity not in ['null', 0]:
            original_price = float(original_price)
            price = float(price)
            descuento_porcentaje = ((original_price - price) / original_price) * 100

            if descuento_porcentaje >= 20:
                items_con_descuento.append({
                    'title': title,
                    'price': price,
                    'original_price': original_price,
                    'descuento_porcentaje': descuento_porcentaje,
                    'permalink': permalink,
                    'thumbnail': thumbnail
                })


   # si hay contenido guardado, enviar email
    if items_con_descuento:
        email_sender = 'agrega el email tuyo' 
        email_password = Variable.get("passw_email") #Agregar una nueva clave con el password, desde el administrador de airflow
        email_receiver = 'agrega el mail del cliente'

        subject = 'Hola estimado cliente, su nuevo celular lo espera'


        # Formato HTML del cuerpo del correo
        body_header = """
        <html>
            <body>
                <p>Tenemos ofertas increíbles que ofrecerle esta semana, Por favor sientase libre de explorar los productos que les recomendamos. Saludos cordiales:</p>
                <ul>
        """

        body_items = ''.join(
            f'<li>{item["title"]}<br>'
            f'<img src="{item["thumbnail"]}" style="max-width: 300px;"><br>'
            f'<a href="{item["permalink"]}">Ver oferta</a></li>'
            for item in items_con_descuento
        )

        body_footer = """
                </ul>
            </body>
        </html>
        """

        body = f"{body_header}{body_items}{body_footer}"

        print(body)


        em = EmailMessage()
        em['From'] = email_sender
        em['To'] = email_receiver
        em['Subject'] = subject
        em.add_alternative(body, subtype='html')  # Indicar que el cuerpo es HTML

        context = ssl.create_default_context()

        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            try:
                smtp.login(email_sender, email_password)
                smtp.sendmail(email_sender, email_receiver, em.as_string())
                print("Correo electrónico enviado con éxito.")
            except Exception as e:
                print(f"Error al enviar el correo electrónico: {str(e)}")


if __name__ == "__main__":
    # Cargar datos desde el archivo TSV
    archivo_tsv = 'D:/Facultad/cursos/CursoPDE/Airflow-ej-tecnico/plugins/tmp/ofertas.tsv'
    with open(archivo_tsv, 'r') as f:
        lineas = f.readlines()

    # Procesar las líneas del TSV y convertirlas a un formato que la función pueda entender
    resultado_tarea_1 = []
    for linea in lineas:
        campos = linea.strip().split('\t')
        resultado_tarea_1.append({
            'title': campos[0],
            'price': campos[1],
            'original_price': campos[2],
            'available_quantity': campos[3],
            'permalink': campos[4],
            'thumbnail': campos[5],
        })

    # Llamar a la función con los datos cargados
    analizar_y_ofertar(resultado_tarea_1)