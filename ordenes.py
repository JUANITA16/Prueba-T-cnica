import json
import uuid
import datetime
import boto3
import pytz
import psycopg2
from pydantic import BaseModel
from typing import Optional
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from botocore.exceptions import ClientError


from src.config.env import SECRET_DBOZONO_CREDENTIALS, LAMBDA_CDT_FILES, BUCKET_CDT_FILES
from src.config.logger import logger
from src.functions.routers.clientes import find_cliente_ozono, insert_cliente
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

class Especie(BaseModel):
    Base_Liquidacion: str
    Emisor: str
    Fecha_Emision: str
    Fecha_Vencimiento: str
    Modalidad: str
    Nemotecnico: str
    Nemotecnico_Hija: str
    Periodicidad: str
    Tasa_Nominal: str
    Tasa_Referencia: str
    Tasa_Efectiva: str
    Descripcion_Titulo: str
    Monto_Inicial: str
    usuario: str
    creationDate: str

class Orden(BaseModel):
    idClient: str
    idTypeClient: str
    contraparte: str
    valorNominal: str
    formaPago: str
    precio: str
    usuario: str
    creationDate: str
    especie: Especie
    cliente: Optional[Cliente]


"""
I'm trying to get the idEspecie from the table especies_cdt.tblespecie, then use that idEspecie to
get the orders from the table flujos_cdt.tblordenes

:param Base_Liquidacion: The base on which the interest is calculated
:param Emisor: The issuer of the security
:param Fecha_Emision: The date the bond was issued
:param Fecha_Vencimiento: The date the bond expires
:param Modalidad: "CDT"
:param Nemotecnico: The name of the CDT
:param Nemotecnico_Hija: Nemotecnico_Hija
:param Periodicidad: "SEMANAL"
:param Tasa_Nominal: The nominal interest rate
:param Tasa_Referencia: The reference rate is the rate of interest that is used as a basis for
calculating other interest rates
:param Tasa_Efectiva: The effective interest rate of the CDT
:param Descripcion_Titulo: "CDT BANCO DE BOGOTA"
:return: a JSONResponse object.
"""
@router.get("")
def get_orders(Base_Liquidacion, Emisor, Fecha_Emision, Fecha_Vencimiento, Modalidad,
               Nemotecnico, Nemotecnico_Hija,  Periodicidad, Tasa_Nominal, Tasa_Referencia, Tasa_Efectiva,  Descripcion_Titulo):
    logger.info("#INICIO get_orders")

    especie = Especie(Base_Liquidacion=Base_Liquidacion, Emisor=Emisor, Fecha_Emision=Fecha_Emision, Fecha_Vencimiento=Fecha_Vencimiento,
                      Modalidad=Modalidad, Nemotecnico=Nemotecnico, Nemotecnico_Hija=Nemotecnico_Hija,
                      Periodicidad=Periodicidad, Tasa_Nominal=Tasa_Nominal, Tasa_Referencia=Tasa_Referencia,
                      Tasa_Efectiva=Tasa_Efectiva, Descripcion_Titulo=Descripcion_Titulo, Monto_Inicial="", usuario="", creationDate="")

    response = get_specie_by_fields(especie)
    result = json.loads(response.body.decode())

    logger.info(result)

    if not result:
        content = []
        status_code = 200
    else:
        idEspecie = result[0]["idEspecie"]
        logger.info(idEspecie)
        try:
            credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
            credentials = json.loads(credentials)
            logger.info('se obtuvo el secreto correctamente')

            conn = psycopg2.connect(**credentials)
            logger.info('conexion exitosa')
            try:
                cur = conn.cursor()

                table_ordenes = "flujos_cdt.tblordenes o"
                table_clientes = "flujos_cdt.tblclientes c"
                join_ordenes_clientes = "ON (o.idclient = c.docclient AND o.idtypeclient = c.idtypeclient)"

                select_query = f"SELECT o.*,c.cuentainversionista FROM {table_ordenes} LEFT JOIN {table_clientes} {join_ordenes_clientes} WHERE o.idespecie = '{idEspecie}' "

                cur.execute(select_query)

                content = []
                rows = cur.fetchall()

                if rows is not None:
                    for row in rows:
                        info = {
                            "idOrden": str(row[0]),
                            "idclient": str(row[1]),
                            "idtypeclient": str(row[2]),
                            "contraparte": str(row[3]),
                            "valornominal": str(row[4]),
                            "formapago": str(row[5]),
                            "precio": str(row[6]),
                            "usuario": str(row[7]),
                            "creationDate": str(row[8]),
                            "idespecie": str(row[9]),
                            "cuentasinversionista": str(row[10])
                        }
                        content.append(info)

                status_code = 200

            except Exception as e:
                logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
                content = {
                    "message": f'Ocurrió un error en la funcion get_orders. Error: {str(e)}'}
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
                "message": f'Ocurrió un error en la funcion get_orders. Error: {str(e)}'}
            logger.info(content)
            status_code = 500

    logger.info("### FIN get_orders ###")

    return JSONResponse(content=content, status_code=status_code)


"""
It connects to a PostgreSQL database, executes a query, and returns the result as a JSON response.

:param idOrden: The id of the order you want to get
:return: a JSONResponse object.
"""
def get_order_by_id(idOrden):
    logger.info("#INICIO get_order_by_id")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('Se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            logger.info("Inicio ejecucion ")

            cur = conn.cursor()

            table_ordenes = "flujos_cdt.tblordenes"

            select_query = f"SELECT * FROM {table_ordenes} WHERE idOrden = {idOrden} "

            cur.execute(select_query)

            logger.info("Query executed")

            content = []
            rows = cur.fetchall()

            if rows is not None:
                for row in rows:
                    info = {
                        "idOrden": str(row[0]),
                        "idclient": str(row[1]),
                        "idtypeclient": str(row[2]),
                        "contraparte": str(row[3]),
                        "valornominal": str(row[4]),
                        "formapago": str(row[5]),
                        "precio": str(row[6]),
                        "usuario": str(row[7]),
                        "creationDate": str(row[8]),
                        "idespecie": str(row[9]),
                    }
                    content.append(info)

            status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion get_order_by_id. Error: {str(e)}'}
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
            "message": f'Ocurrió un error en la funcion get_order_by_id. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN get_order_by_id ###")

    return JSONResponse(content=content, status_code=status_code)


"""
It inserts a new order into the database, and if the order's species doesn't exist, it inserts the
species as well.

:param item: Orden
:type item: Orden
:return: a JSONResponse object.
"""
@router.post("")
def insert_orden(item: Orden):
    logger.info("### INICIO insert_orden ###")
    logger.info(item)
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            cur = conn.cursor()
            idClient = item.idClient
            idTypeClient = item.idTypeClient
            if idTypeClient == "NIT":
                idClient = idClient + item.cliente.verificationNum
            responseClient = find_cliente_ozono(idClient, idTypeClient)
            resultClient = json.loads(responseClient.body.decode())
            logger.info(resultClient)

            if not resultClient:
                responseClient = insert_cliente(item.cliente)
                resultClient = json.loads(responseClient.body.decode())
                logger.info(resultClient)

            contraparteCompleto = item.contraparte
            split = contraparteCompleto.split(" - ")
            contraparte = split[0]
            valorNominal = item.valorNominal
            formaPago = item.formaPago
            precio = item.precio
            usuario = item.usuario
            creationDate = datetime.datetime.now(tz=pytz.timezone('America/Bogota')).strftime('%d/%m/%Y-%H:%M:%S')

            nemotecnico = item.especie.Nemotecnico

            table_ordenes = "flujos_cdt.tblOrdenes"
            table_especies = "flujos_cdt.tblEspecies"

            insert_sql_orden = f''' 
                    INSERT INTO {table_ordenes} (idClient, idTypeClient, contraparte, valorNominal, formaPago, precio, usuario, creationDate, idEspecie)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''

            response = get_specie_by_fields(item.especie)
            result = json.loads(response.body.decode())
            logger.info(result)

            if not result:

                monto = item.especie.Monto_Inicial
                if float(monto) < float(valorNominal):
                    raise Exception(
                        "El valor de la orden excede el monto inicial")

                logger.info("Insertando especie y orden")
                idEspecie = uuid.uuid1().__str__()
                emisor = item.especie.Emisor
                nemotecnicohija = item.especie.Nemotecnico_Hija
                fechaemision = item.especie.Fecha_Emision
                fechavencimiento = item.especie.Fecha_Vencimiento
                tasanominal = item.especie.Tasa_Nominal
                tasaefectiva = item.especie.Tasa_Efectiva
                tasareferencia = item.especie.Tasa_Referencia
                periodicidad = item.especie.Periodicidad
                modalidad = item.especie.Modalidad
                baseliquidacion = item.especie.Base_Liquidacion
                descripcion = item.especie.Descripcion_Titulo
                monto = float(monto) - float(valorNominal)

                values_orden = (idClient, idTypeClient, contraparte,
                                valorNominal, formaPago, precio, usuario, creationDate, idEspecie)

                values_especie = (idEspecie, emisor, nemotecnico, nemotecnicohija, fechaemision, fechavencimiento,
                                  tasanominal, tasaefectiva, tasareferencia, periodicidad, modalidad, baseliquidacion, descripcion, monto, usuario, creationDate)

                insert_sql_especie = f''' 
                    INSERT INTO {table_especies} (idEspecie, emisor,nemotecnico,nemotecnicohija,fechaemision,fechavencimiento,tasanominal,tasaefectiva, 
                                                    tasareferencia,periodicidad, modalidad,baseliquidacion, descripcionTitulo,montoInicial,usuario, creationDate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
                '''
                cur.execute(insert_sql_especie, values_especie)
                cur.execute(insert_sql_orden, values_orden)

            else:
                idEspecie = result[0]["idEspecie"]
                logger.info("Insertando solo orden")

                logger.info(idEspecie)

                responseMonto = get_monto(idEspecie)
                resultMonto = json.loads(responseMonto.body.decode())
                logger.info(resultMonto)

                if float(resultMonto[0]) < float(valorNominal):
                    raise Exception(
                        "El valor de la orden excede el monto inicial")

                values_orden = (idClient, idTypeClient, contraparte,
                                valorNominal, formaPago, precio, usuario, creationDate, idEspecie)
                logger.info(idEspecie)

                cur.execute(insert_sql_orden, values_orden)

                new_val = float(resultMonto[0]) - float(valorNominal)
                update_Especie(idEspecie, new_val)

            conn.commit()
            content = {"message": f'Proceso de insercion exitoso'}
            status_code = 200
        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion insert_orden. Error: {str(e)}'}
            logger.info(content)
            if (str(e) == "El valor de la orden excede el monto inicial"):
                status_code = 409
            else:
                status_code = 500
        finally:
            if conn:
                cur.close()
                conn.close()
                logger.info("PostgreSQL connection is closed")
    except Exception as e:
        logger.error(f'Ocurrió un error conectando a la bd: {str(e)}')
        content = {
            "message": f'Ocurrió un error en la funcion insert_orden. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN insert_orden ###")
    return JSONResponse(content=content, status_code=status_code)


"""
It updates a row in a table in a PostgreSQL database

:param idOrden: The id of the order to be updated
:param item: Orden
:type item: Orden
:return: a JSONResponse object.
"""
@router.put("")
def update_order(idOrden, item: Orden):
    logger.info("#INICIO update_order")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)

        logger.info('conexion exitosa')
        try:
            cur = conn.cursor()

            idClient = item.idClient
            idTypeClient = item.idTypeClient
            contraparte = item.contraparte
            valorNominal = item.valorNominal
            formaPago = item.formaPago
            precio = item.precio

            table_ordenes = "flujos_cdt.tblordenes"

            response = get_specie_by_fields(item.especie)
            result = json.loads(response.body.decode())
            logger.info(result)

            idEspecie = result[0]["idEspecie"]

            logger.info(idEspecie)

            responseMonto = get_monto(idEspecie)
            resultMonto = json.loads(responseMonto.body.decode())
            logger.info(resultMonto)

            if float(resultMonto[0]) < float(valorNominal):
                raise Exception("El valor de la orden excede el monto inicial")
            else:
                update_query = f"UPDATE {table_ordenes} SET idclient = '{idClient}', idtypeclient = '{idTypeClient}' , contraparte = '{contraparte}', valornominal = {valorNominal}, formapago = '{formaPago}', precio = {precio} WHERE idorden = {idOrden}"

                cur.execute(update_query)

                responseOrden = get_order_by_id(idOrden)
                resultOrden = json.loads(responseOrden.body.decode())
                logger.info(resultOrden)

                prev_val = resultOrden[0]["valornominal"]
                logger.info(prev_val)
                changed_val = float(valorNominal) - float(prev_val)
                logger.info(changed_val)

                new_val = float(resultMonto[0]) - float(changed_val)
                update_Especie(idEspecie, new_val)

                conn.commit()

                content = {"message": f'Proceso de actualizacion exitoso'}
                status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion update_order. Error: {str(e)}'}
            if (str(e) == "El valor de la orden excede el monto inicial"):
                status_code = 409
            else:
                status_code = 500
            logger.info(content)
        finally:
            if conn:
                cur.close()
                conn.close()
                logger.info("PostgreSQL connection is closed")
    except Exception as e:
        logger.error(f'Ocurrió un error conectando a la bd: {str(e)}')
        content = {
            "message": f'Ocurrió un error en la funcion update_order. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN update_order ###")
    return JSONResponse(content=content, status_code=status_code)


"""
It deletes a row from a table in a PostgreSQL database

:param id: The id of the order to delete
:return: a JSONResponse object.
"""
@router.delete("")
def delete_order(id):
    logger.info("#INICIO delete_order")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            
            responseOrden = get_order_by_id(id)
            resultOrden = json.loads(responseOrden.body.decode())
            logger.info(resultOrden)
            
            prev_val = resultOrden[0]["valornominal"]
            idEspecie = resultOrden[0]["idespecie"]
            logger.info(prev_val)

            responseMonto = get_monto(idEspecie)
            resultMonto = json.loads(responseMonto.body.decode())
            logger.info(resultMonto)

            new_val = float(resultMonto[0]) +  float(prev_val)
            update_Especie(idEspecie, new_val)

            cur = conn.cursor()

            table_ordenes = "flujos_cdt.tblordenes"

            query = f"DELETE FROM {table_ordenes} WHERE idorden = {id}"

            logger.info("Statement to execute: " + query)

            cur.execute(query)

            conn.commit()

            content = {"message": f'Proceso de eliminacion exitoso'}
            status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion delete_order. Error: {str(e)}'}
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
            "message": f'Ocurrió un error en la funcion delete_order. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN delete_order ###")
    return JSONResponse(content=content, status_code=status_code)



"""
It inserts a new record into a table if the record doesn't exist, or updates the record if it does
exist.

:param item: Especie
:type item: Especie
:return: a JSONResponse object.
"""
@router.post("/especie")
def insert_Especie(item: Especie):
    logger.info("### INICIO insert_Especie ###")
    logger.info(item)
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            cur = conn.cursor()
            usuario = item.usuario
            creationDate = datetime.datetime.now().strftime('%d/%m/%Y-%H:%M:%S')

            nemotecnico = item.Nemotecnico

            table_especies = "flujos_cdt.tblEspecies"

            response = get_specie_by_fields(item)

            result = json.loads(response.body.decode())

            logger.info(result)

            monto = item.Monto_Inicial

            if not result:
                logger.info("Insertando especie")
                idEspecie = uuid.uuid1().__str__()
                emisor = item.Emisor
                nemotecnicohija = item.Nemotecnico_Hija
                fechaemision = item.Fecha_Emision
                fechavencimiento = item.Fecha_Vencimiento
                tasanominal = item.Tasa_Nominal
                tasaefectiva = item.Tasa_Efectiva
                tasareferencia = item.Tasa_Referencia
                periodicidad = item.Periodicidad
                modalidad = item.Modalidad
                baseliquidacion = item.Base_Liquidacion
                descripcion = item.Descripcion_Titulo

                values_especie = (idEspecie, emisor, nemotecnico, nemotecnicohija, fechaemision, fechavencimiento,
                                  tasanominal, tasaefectiva, tasareferencia, periodicidad, modalidad, baseliquidacion, descripcion, monto, usuario, creationDate)

                insert_sql_especie = f''' 
                    INSERT INTO {table_especies} (idEspecie, emisor,nemotecnico,nemotecnicohija,fechaemision,fechavencimiento,tasanominal,tasaefectiva,tasareferencia,
                                                    periodicidad, modalidad,baseliquidacion, descripcionTitulo,montoInicial,usuario, creationDate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
                '''
                cur.execute(insert_sql_especie, values_especie)

                content = {"message": f'Proceso de insercion exitoso'}

            else:
                logger.info("Actualizando el valor del monto inicial")
                idEspecie = result[0]["idEspecie"]
                logger.info(idEspecie)
                update_query = f"update {table_especies} set montoInicial = {monto} where idespecie = '{idEspecie}'"

                cur.execute(update_query)

                content = {"message": f'Proceso de actualizacion exitoso'}

            conn.commit()
            status_code = 200
        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion insert_Especie. Error: {str(e)}'}
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
            "message": f'Ocurrió un error en la funcion insert_Especie. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN insert_Especie ###")
    return JSONResponse(content=content, status_code=status_code)


"""
It connects to a PostgreSQL database, executes a query and returns the result.

:param id: The id of the especie
:return: A JSONResponse object
"""
@router.get("/especie")
def get_monto(id):
    logger.info("#INICIO get_monto")
    logger.info(id)
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('Se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:

            cur = conn.cursor()

            table_especies = "flujos_cdt.tblEspecies"

            query = f"select montoInicial from {table_especies} where idEspecie = '{id}' "

            cur.execute(query)

            logger.info("Query executed")

            content = []
            rows = cur.fetchall()

            logger.info(rows)

            if rows is not None:
                for row in rows:
                    content.append(str(row[0]))

            logger.info(content)

            status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion get_monto. Error: {str(e)}'}
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
            "message": f'Ocurrió un error en la funcion get_monto. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN get_monto ###")

    return JSONResponse(content=content, status_code=status_code)


"""
It updates the value of the montoInicial column in the tblEspecies table.

:param id: "1"
:param monto: the value to be updated
:return: a JSONResponse object.
"""
@router.put("/especie")
def update_Especie(id, monto):
    logger.info("### INICIO update_Especie ###")
    logger.info(id)
    logger.info(monto)
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            cur = conn.cursor()

            table_especies = "flujos_cdt.tblEspecies"

            logger.info("Actualizando el valor del monto inicial")

            update_query = f"update {table_especies} set montoInicial = {monto} where idespecie = '{id}'"

            cur.execute(update_query)

            content = {"message": f'Proceso de actualizacion exitoso'}

            conn.commit()
            status_code = 200
        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion update_Especie. Error: {str(e)}'}
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
            "message": f'Ocurrió un error en la funcion update_Especie. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN update_Especie ###")
    return JSONResponse(content=content, status_code=status_code)


"""
It connects to a PostgreSQL database, executes a query and returns the result as a JSON object.

:param item: Especie
:type item: Especie
:return: a JSONResponse object.
"""
@router.post("/nemotecnico")
def get_specie_by_fields(item: Especie):
    logger.info("#INICIO get_specie_by_fields")
    logger.info(item)
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            cur = conn.cursor()

            table_especies = "flujos_cdt.tblEspecies"

            select_query = f''' 
                    select * from {table_especies} where nemotecnico = '{item.Nemotecnico}' and emisor = '{item.Emisor}' and nemotecnicohija = '{item.Nemotecnico_Hija}' and fechaemision = '{item.Fecha_Emision}'
                    and fechavencimiento = '{item.Fecha_Vencimiento}' and tasanominal = {item.Tasa_Nominal} and tasaefectiva = '{item.Tasa_Efectiva}' and 
                    tasareferencia = '{item.Tasa_Referencia}' and periodicidad = '{item.Periodicidad}' and modalidad  = '{item.Modalidad}'
                    and baseliquidacion = {item.Base_Liquidacion} and descripciontitulo = '{item.Descripcion_Titulo}'
                '''

            cur.execute(select_query)

            content = []
            rows = cur.fetchall()

            if rows is not None:
                for row in rows:
                    info = {
                        "idEspecie": str(row[0]),
                        "emisor": str(row[1]),
                        "nemotecnico": str(row[2]),
                        "nemotecnicoHija": str(row[3]),
                        "fechaEmision": str(row[4]),
                        "fechaVencimiento": str(row[5]),
                        "tasaNominal": str(row[6]),
                        "tasaEfectiva": str(row[7]),
                        "tasaReferencia": str(row[8]),
                        "periodicidad": str(row[9]),
                        "modalidad": str(row[10]),
                        "baseLiquidacion": str(row[11]),
                        "descripcionTitulo": str(row[12]),
                        "montoInicial": str(row[13]),
                        "usuario": str(row[14]),
                        "creationDate": str(row[15]),

                    }
                    content.append(info)

            status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion get_specie_by_fields. Error: {str(e)}'}
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
            "message": f'Ocurrió un error en la funcion get_specie_by_fields. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN get_specie_by_fields ###")
    return JSONResponse(content=content, status_code=status_code)



"""
It inserts a row into a table in a PostgreSQL database

:param idArchivo: 1
:param filename: the name of the file
:param user: the user who uploaded the file
:param status: 0 = success, 1 = error
:param statusCode: 0
:param filedate: 2020-01-01
:return: a JSONResponse object.
"""
@router.post("/insertFile")
def insert_file(idArchivo, filename, user, status, statusCode, filedate):
    logger.info("#INICIO insert_file")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            cur = conn.cursor()

            table_archivos = "flujos_cdt.tblarchivos"

            date = datetime.datetime.now(tz=pytz.timezone("America/Bogota")).strftime('%Y-%m-%d %H:%M:%S')

            values_archivo = (idArchivo, filename, user,
                              date, status, statusCode, filedate)

            insert_archivo = f''' 
                    insert into {table_archivos} (idArchivo, filename, usuario, creationdate, status, statuscode, fechaOrden)
                    VALUES (%s, %s, %s, %s, %s, %s ,%s)
                '''

            cur.execute(insert_archivo, values_archivo)

            conn.commit()
            content = {"message": f'Proceso de insercion exitoso'}
            status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion insert_file. Error: {str(e)}'}
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
            "message": f'Ocurrió un error en la funcion insert_file. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN insert_file ###")

    return JSONResponse(content=content, status_code=status_code)



"""
It updates a row in a table in a PostgreSQL database

:param idArchivo: The id of the file to be updated
:param status: "OK"
:param statusCode: 200
:return: a JSONResponse object.
"""
@router.put("/archivos")
def update_file(idArchivo, status, statusCode):
    logger.info("#INICIO update_file")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)

        logger.info('conexion exitosa')
        try:
            cur = conn.cursor()

            table_archivos = "flujos_cdt.tblarchivos"

            update_query = f"UPDATE {table_archivos} SET status = '{status}', statusCode = '{statusCode}' WHERE idArchivo = '{idArchivo}'"

            logger.info("Statement to execute: " + update_query)

            cur.execute(update_query)

            conn.commit()

            content = {"message": f'Proceso de actualizacion exitoso'}
            status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion update_file. Error: {str(e)}'}
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
            "message": f'Ocurrió un error en la funcion update_file. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN update_file ###")
    return JSONResponse(content=content, status_code=status_code)


"""
It takes a date and a user, and then it calls a lambda function.

:param fecha: "2020-01-01"
:param usuario: user
:return: The response from the lambda function.
"""
@router.post("/archivos")
def generate_file(fecha, usuario):
    logger.info("### INICIO generar_archivo ###")
    logger.info(fecha)
    try:
        lambda_name = LAMBDA_CDT_FILES
        lambda_client = boto3.client('lambda')
        logger.info("cliente lambda creado")

        mes = fecha[5:7]
        dia = fecha[8:10]
        logger.info(mes)
        logger.info(dia)
        filename = "P664" + mes + dia + ".001"
        idArchivo = uuid.uuid1().__str__()
        lambda_payload = {"fecha": fecha,
                          "filename": filename, "idArchivo": idArchivo}

        logger.info("insertando en la bd ")
        fecha = fecha[0:10]
        insertResponse = insert_file(
            idArchivo, filename, usuario, "En ejecución", 102, fecha)
        insertResult = json.loads(insertResponse.body.decode())
        logger.info(f'Resultado: {insertResult}')

        response = lambda_client.invoke(FunctionName=lambda_name,
                                        InvocationType='Event',
                                        Payload=json.dumps(lambda_payload))
        logger.info(response)
        logger.info(response['Payload'].read())

        if (response['StatusCode'] != 200 and response['StatusCode'] != 202):
            content = {
                "message": f'Ocurrió un error en la lambda: {lambda_name}.'}
            status_code = response['StatusCode']
            logger.error(f'Ocurrió un error en la lambda: {lambda_name}.')
        else:
            content = {
                "message": f'Lambda invocada correctamente'}
            status_code = 200
    except Exception as e:
        logger.error(
            f'Ocurrió un error en la funcion generar_archivo. Error: {str(e)}')
        content = {
            "message": f'Ocurrió un error en la funcion generar_archivo. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN generate_file ###")
    return JSONResponse(content=content, status_code=status_code)


"""
It connects to a PostgreSQL database, executes a query, and returns the result as a JSON response
:return: a JSONResponse object.
"""
@router.get("/archivos")
def get_files():
    logger.info("#INICIO get_files")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('Se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            logger.info("Inicio ejecucion ")

            cur = conn.cursor()

            table_archivos = "flujos_cdt.tblarchivos"

            select_query = f"SELECT * FROM {table_archivos}"

            cur.execute(select_query)

            logger.info("Query executed")

            content = []
            rows = cur.fetchall()

            logger.info(rows)

            if rows is not None:
                for row in rows:
                    info = {
                        "idArchivo": str(row[0]),
                        "filename": str(row[1]),
                        "usuario": str(row[2]),
                        "status": str(row[3]),
                        "statusCode": str(row[4]),
                        "fechaOrdenes": str(row[5]),
                        "creationDate": str(row[6]),
                    }
                    content.append(info)

            status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion get_files. Error: {str(e)}'}
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
            "message": f'Ocurrió un error en la funcion get_files. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN get_files ###")

    return JSONResponse(content=content, status_code=status_code)


"""
It connects to a PostgreSQL database, executes a query, and returns the results as a JSON response

:param fecha: 2020-01-01
:return: a JSONResponse object.
"""
@router.get("/archivosfecha")
def get_files_by_date(fecha):
    logger.info("#INICIO get_files")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('Se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            logger.info("Inicio ejecucion ")

            cur = conn.cursor()

            table_archivos = "flujos_cdt.tblarchivos"

            fechaf = fecha[0:10]
            logger.info(fechaf)

            select_query = f"SELECT * FROM {table_archivos} where fechaOrden like '{fechaf}%'"

            cur.execute(select_query)

            logger.info("Query executed")

            content = []
            rows = cur.fetchall()

            logger.info(rows)

            if rows is not None:
                for row in rows:
                    info = {
                        "idArchivo": str(row[0]),
                        "filename": str(row[1]),
                        "usuario": str(row[2]),
                        "status": str(row[3]),
                        "statusCode": str(row[4]),
                        "fechaOrdenes": str(row[5]),
                        "creationDate": str(row[6]),
                    }
                    content.append(info)

            status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion get_files. Error: {str(e)}'}
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
            "message": f'Ocurrió un error en la funcion get_files. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN get_files ###")

    return JSONResponse(content=content, status_code=status_code)


"""
It generates a presigned URL that can be used to download a file from an S3 bucket

:param key: The name of the file to download
:return: The presigned URL
"""
@router.get("/descargar")
def download_file(key):
    s3_client = boto3.client('s3')
    try:
        bucket_name = BUCKET_CDT_FILES
        logger.info(bucket_name)
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': key},
                                                    ExpiresIn=60)
        content = response
        status_code = 200
    except ClientError as e:
        logger.error(e)
        content = {
            "message": f'Ocurrió un error en la funcion download_file. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    # The response contains the presigned URL
    return JSONResponse(content=content, status_code=status_code)


"""
It connects to a PostgreSQL database, executes a query, and returns the result as a JSON response
:return: A JSONResponse object
"""
@router.get("/contraparte")
def get_contrapartes():
    logger.info("#INICIO get_contrapartes")
    try:
        credentials = utils.get_secret_value(SECRET_DBOZONO_CREDENTIALS)
        credentials = json.loads(credentials)
        logger.info('Se obtuvo el secreto correctamente')

        conn = psycopg2.connect(**credentials)
        logger.info('conexion exitosa')
        try:
            logger.info("Inicio ejecucion ")

            cur = conn.cursor()

            table_emisores = "flujos_cdt.\"tblMaestroAnexoDepositantesDirectosDeceval\""

            select_query = f"SELECT descripcion, nit FROM {table_emisores}"

            cur.execute(select_query)

            logger.info("Query executed")

            content = []
            rows = cur.fetchall()

            if rows is not None:
                for row in rows:
                    final = str(row[0]) + " - " + str(row[1])
                    content.append(final)

            status_code = 200

        except Exception as e:
            logger.error(f"Error ejecutando la query PostgreSQL  {str(e)}")
            content = {
                "message": f'Ocurrió un error en la funcion get_contrapartes. Error: {str(e)}'}
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
            "message": f'Ocurrió un error en la funcion get_contrapartes. Error: {str(e)}'}
        logger.info(content)
        status_code = 500

    logger.info("### FIN get_contrapartes ###")

    return JSONResponse(content=content, status_code=status_code)
