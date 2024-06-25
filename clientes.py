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
            if not item.verificationNum or item.verificationNum == "None":
                table_clientes = "flujos_cdt.tblclientes"

                update_query = f"update {table_clientes} set docClient = %s, idtypeclient = %s, nombres = %s, cuentainversionista = %s where docclient = %s"
                cur.execute(update_query, (docClient, idTypeClient, nombres, cuenta, prev_doc))
            else:
                verificationNum = item.verificationNum
                docFinal = docClient + verificationNum

                table_clientes = "flujos_cdt.tblclientes"
                update_query = f"update {table_clientes} set docClient = %s, idtypeclient = %s, nombres = %s, cuentainversionista = %s where docclient = %s"
                cur.execute(update_query, (docFinal, idTypeClient, nombres, cuenta, prev_doc))

            logger.info("Statement to execute: " + update_query)

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

            query = f"DELETE FROM {table_clientes} WHERE docClient = %s AND idtypeclient = %s"
            cur.execute(query, (id, doc_type))

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

            if doc_type == 'NIT':
                select_query = f" SELECT * FROM {table_clientes} WHERE idtypeclient = %s AND docclient LIKE %s AND docclient LIKE %s"
                cur.execute(select_query, (doc_type, f'{id}%', f'%{vNum}'))
            else:
                select_query = f" SELECT * FROM {table_clientes} WHERE docClient = %s AND idtypeclient = %s"
                cur.execute(select_query, (id, doc_type))

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


select_query = "SELECT * FROM flujos_cdt.tblclientes"


insert_cliente = ''' 
    INSERT INTO flujos_cdt.tblclientes (docclient, idtypeclient, cuentainversionista, usuario, creationdate, nombres)
    VALUES (%s, %s, %s, %s, %s, %s)
'''
