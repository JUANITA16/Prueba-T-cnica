import json
import datetime
import psycopg2

from pydantic import BaseModel
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from src.config.env import URL_INTEGRATION, PATH_DOMINUS_CLIENTS, SECRET_DBOZONO_CREDENTIALS, SECRET_APIKEY_INTEGRATION
from src.config.logger import logger
from src.utils import utils


router = APIRouter()

class Cliente(BaseModel):
    docClient: str
    idTypeClient: str
    nombres: str
    apellidos: str
    cuentaInversionista: str
    usuario: str
    creationDate: str
    verificationNum: str


"""
I'm trying to insert a new row into a PostgreSQL table using a Python function.

:param item: Cliente
:type item: Cliente
:return: a JSONResponse object.
"""
@router.post("")
def insert_cliente(item: Cliente):
    logger.info("#INICIO insert_cliente")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            cur = conn.cursor()

            table_clientes = "flujos_cdt.tblclientes"

            docClient = item.docClient
            idTypeClient = item.idTypeClient
            nombres = item.nombres
            apellidos = item.apellidos
            cuentaInversionista = item.cuentaInversionista
            usuario = item.usuario
            verificationNum = item.verificationNum
            creationDate = datetime.datetime.now().strftime('%d/%m/%Y-%H:%M:%S')
            nombreFinal = apellidos + " " + nombres
            docFinal = docClient + verificationNum

            values_cliente = (docFinal, idTypeClient, cuentaInversionista,
                              usuario, creationDate, nombreFinal)

            insert_cliente = f''' 
                INSERT INTO {table_clientes} (docclient, idtypeclient, cuentainversionista, usuario, creationdate, nombres)
                VALUES (%s, %s, %s, %s, %s, %s)
            '''

            cur.execute(insert_cliente, values_cliente)

            conn.commit()
            content = {"message": f'Proceso de insercion exitoso'}
            status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion insert_cliente. Error: {str(e)}'}
            logger.info(content)
            status_code = 500
        finally:
            if conn:
                cur.close()
                conn.close()
                logger.info("PostgreSQL connection is closed")
    except Exception as e:
        logger.error(f'Ocurrió un error conectando a la bd: {str(e)}')
        content = {
            "message": f'Ocurrió un error en la funcion insert_cliente. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN insert_cliente ###")

    return JSONResponse(content=content, status_code=status_code)


"""
It connects to a PostgreSQL database, executes a query, and returns the result as a JSON response
:return: a JSONResponse object.
"""
@router.get("")
def get_clientes():
    logger.info("#INICIO get_clientes")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('Se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            logger.info("Inicio ejecucion ")

            cur = conn.cursor()

            table_clientes = "flujos_cdt.tblclientes"

            select_query = f"SELECT * FROM {table_clientes}"

            cur.execute(select_query)

            logger.info("Query executed")

            content = []
            rows = cur.fetchall()

            logger.info(rows)

            content, status_code = format_client(rows)

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion get_clientes. Error: {str(e)}'}
            logger.info(content)
            status_code = 500
        finally:
            if conn:
                cur.close()
                conn.close()
                logger.info("PostgreSQL connection is closed")
    except Exception as e:
        logger.error(f'Ocurrió un error conectando a la bd: {str(e)}')
        content = {
            "message": f'Ocurrió un error en la funcion get_clientes. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN get_clientes ###")

    return JSONResponse(content=content, status_code=status_code)


"""
It connects to a PostgreSQL database, executes a query, and returns the result.

:param id: 123456789
:param doc_type: 1
:return: A JSONResponse object
"""
@router.get("/id")
def find_cliente(id, doc_type, vNum):
    logger.info("#INICIO find_clientes")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('Se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            logger.info("Inicio ejecucion ")

            cur = conn.cursor()

            table_clientes = "flujos_cdt.tblclientes"

            if (doc_type == 'NIT'):
                select_query = f" SELECT * FROM {table_clientes} WHERE idtypeclient = '{doc_type}' and docclient like '{id}%' and docclient like '%{vNum}'"
            else:
                select_query = f" SELECT * FROM {table_clientes} WHERE docClient = '{id}' AND idtypeclient = '{doc_type}' "

            cur.execute(select_query)

            logger.info("Query executed")

            content = []
            rows = cur.fetchall()

            logger.info(rows)
            
            content, status_code = format_client(rows)

            if not rows:
                id = id + vNum
                content, status_code = find_cliente_dominus(id, doc_type)

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion find_clientes. Error: {str(e)}'}
            logger.info(content)
            status_code = 500
        finally:
            if conn:
                cur.close()
                conn.close()
                logger.info("PostgreSQL connection is closed")
    except Exception as e:
        logger.error(f'Ocurrió un error conectando a la bd: {str(e)}')
        content = {
            "message": f'Ocurrió un error en la funcion find_clientes. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN find_clientes ###")

    return JSONResponse(content=content, status_code=status_code)


"""
It makes a GET request to a URL with a header and a parameter

:param id: 123456789
:param doc_type: "DNI"
:return: The content is a dictionary with the following structure:
{
    "id": "string",
    "name": "string",
    "lastName": "string",
    "secondLastName": "string",
    "email": "string",
    "phone": "string",
    "cellphone": "string",
    "address": "string",
"""
def find_cliente_dominus(id, doc_type):
    try:
        logger.info("# INICIO find_cliente_dominus")

        apiKey = utils.get_secret_value(SECRET_APIKEY_INTEGRATION)
        url = URL_INTEGRATION + PATH_DOMINUS_CLIENTS

        headers = {
            "Authorization": apiKey
        }
        params = {
            "Dtype": doc_type,
            "doc": id,
        }
        content, status_code = utils.consume_service(
            "GET", url, headers, params)

        logger.info(f'Resultado contenido Final: {content}')
        logger.info("#FIN find_cliente_dominus")
    except Exception as e:
        content = {
            "message": f'Ocurrió un error en la funcion find_cliente_dominus. Error: {str(e)}'}
        logger.info(content)
        status_code = 500
        logger.error(
            f'Ocurrió un error en la funcion find_cliente_dominus. Error: {str(e)}')

    return content, status_code


"""
It connects to a PostgreSQL database, executes a query, and returns the result as a JSON object

:param id: 123456789
:param doc_type: 1
:return: a JSONResponse object.
"""
def find_cliente_ozono(id, doc_type):
    logger.info("#INICIO find_cliente_ozono")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('Se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            logger.info("Inicio ejecucion ")

            logger.info(id)
            logger.info(doc_type)

            cur = conn.cursor()

            table_clientes = "flujos_cdt.tblclientes"

            select_query = f" SELECT * FROM {table_clientes} WHERE docClient = '{id}' AND idtypeclient = '{doc_type}' "

            cur.execute(select_query)

            logger.info("Query executed")

            rows = cur.fetchall()

            content = []
            logger.info(rows)

            content, status_code = format_client(rows)

        except Exception as e:
            logger.error(
                f"Error ejecutando la query PostgreSQL, Error: {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion find_cliente_ozono. Error: {str(e)}'}
            logger.info(content)
            status_code = 500
        finally:
            if conn:
                cur.close()
                conn.close()
                logger.info("PostgreSQL connection is closed")
    except Exception as e:
        logger.error(f'Ocurrió un error conectando a la bd: {str(e)}')
        content = {
            "message": f'Ocurrió un error en la funcion find_cliente_ozono. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN find_cliente_ozono ###")

    return JSONResponse(content=content, status_code=status_code)


"""
It deletes a row from a table in a PostgreSQL database

:param id: 123456789
:param doc_type: The type of document that the client has
:return: a JSONResponse object.
"""
@router.delete("")
def delete_clientes(id, doc_type):
    logger.info("#INICIO delete_clientes")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            cur = conn.cursor()

            table_clientes = "flujos_cdt.tblclientes"

            query = f"DELETE FROM {table_clientes} WHERE docClient = '{id}' AND idtypeclient = '{doc_type}' "

            logger.info("Statement to execute: " + query)

            cur.execute(query)

            conn.commit()

            content = {"message": f'Proceso de eliminacion exitoso'}
            status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion delete_clientes. Error: {str(e)}'}
            logger.info(content)
            status_code = 500
        finally:
            if conn:
                cur.close()
                conn.close()
                logger.info("PostgreSQL connection is closed")
    except Exception as e:
        logger.error(f'Ocurrió un error conectando a la bd: {str(e)}')
        content = {
            "message": f'Ocurrió un error en la funcion delete_clientes. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN delete_clientes ###")
    return JSONResponse(content=content, status_code=status_code)


"""
It updates a row in a table in a PostgreSQL database

:param item: Cliente
:type item: Cliente
:param prev_doc: the previous document number
:return: a JSONResponse object.
"""
@router.put("")
def update_clientes(item: Cliente, prev_doc):
    logger.info("#INICIO update_clientes")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)

        logger.info('conexion exitosa')
        try:
            cur = conn.cursor()

            docClient = item.docClient
            idTypeClient = item.idTypeClient
            nombres = item.nombres
            cuenta = item.cuentaInversionista
            if (not item.verificationNum or item.verificationNum == "None"):
                table_clientes = "flujos_cdt.tblclientes"

                update_query = f"update {table_clientes} set docClient = '{docClient}', idtypeclient = '{idTypeClient}', nombres = '{nombres}', cuentainversionista = '{cuenta}' where docclient = '{prev_doc}'"
            else:
                verificationNum = item.verificationNum
                docFinal = docClient+verificationNum

                table_clientes = "flujos_cdt.tblclientes"
                update_query = f"update {table_clientes} set docClient = '{docFinal}', idtypeclient = '{idTypeClient}', nombres = '{nombres}', cuentainversionista = '{cuenta}' where docclient = '{prev_doc}'"

            logger.info("Statement to execute: " + update_query)

            cur.execute(update_query)

            conn.commit()

            content = {"message": f'Proceso de actualizacion exitoso'}
            status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion update_clientes. Error: {str(e)}'}
            logger.info(content)
            status_code = 500
        finally:
            if conn:
                cur.close()
                conn.close()
                logger.info("PostgreSQL connection is closed")
    except Exception as e:
        logger.error(f'Ocurrió un error conectando a la bd: {str(e)}')
        content = {
            "message": f'Ocurrió un error en la funcion update_clientes. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN update_clientes ###")
    return JSONResponse(content=content, status_code=status_code)

def format_client(rows):
    content = []

    logger.info(rows)

    if rows is not None:
        for row in rows:
            doc = str(row[1])
            doc_type = str(row[2])
            if ( doc_type != 'NIT'):
                info = {
                    "idClient": str(row[0]),
                    "docClient": doc,
                    "idTypeClient": str(row[2]),
                    "cuentaInversionista": str(row[3]),
                    "usuario": str(row[4]),
                    "creationDate": str(row[5]),
                    "nombres": str(row[6]),
                    "verificationNum": "",
                }
            else:
                info = {
                    "idClient": str(row[0]),
                    "docClient": doc[:-1],
                    "idTypeClient": str(row[2]),
                    "cuentaInversionista": str(row[3]),
                    "usuario": str(row[4]),
                    "creationDate": str(row[5]),
                    "nombres": str(row[6]),
                    "verificationNum": doc[-1],
            }
            content.append(info)

    status_code = 200

    return(content, status_code)
