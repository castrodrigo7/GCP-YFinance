<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Proyecto de Asesor Financiero - GCP</title>
</head>
<body>
    <h1>Proyecto de Asesor Financiero - GCP</h1>
    <p>Este proyecto en Google Cloud Platform (GCP) permite analizar datos financieros en tiempo real de empresas del índice NASDAQ-100. La solución emplea infraestructura en la nube para la ingesta, almacenamiento y análisis de datos, y utiliza modelos de Machine Learning para extraer insights financieros.</p>
    
    <h2>Objetivo</h2>
    <p>El sistema está diseñado para procesar y analizar datos bursátiles históricos y en tiempo real de manera eficiente. Sus funcionalidades clave incluyen:</p>
    <ul>
        <li><strong>Ingesta de datos</strong> sobre la capitalización bursátil de empresas.</li>
        <li><strong>Almacenamiento y consulta rápida</strong> en BigQuery.</li>
        <li><strong>Visualización de datos</strong> mediante Power BI y Looker Studio, facilitando reportes y gráficos dinámicos.</li>
    </ul>
    
    <h2>Tecnologías Utilizadas</h2>
    <p>Para crear una arquitectura escalable y automatizada en la nube, se utilizan:</p>
    <ul>
        <li><strong>Compute Engine (VM):</strong> Para el procesamiento inicial y la ingesta de datos.</li>
        <li><strong>BigQuery:</strong> Para almacenar grandes volúmenes de datos y realizar consultas eficientes.</li>
        <li><strong>Python y YFinance:</strong> Para la extracción y procesamiento de datos financieros.</li>
        <li><strong>SQL:</strong> Para consultas en BigQuery.</li>
        <li><strong>Cloud Functions:</strong> Para la actualización diaria automática de datos.</li>
        <li><strong>Looker Studio y Power BI:</strong> Para visualización y generación de reportes.</li>
    </ul>
    
    <h2>Guía de Instalación</h2>
    
    <h3>1. Configuración de la Máquina Virtual (VM)</h3>
    <p>Crea una VM en Compute Engine de Google Cloud. Instala Python y las librerías necesarias como <code>yfinance</code>, <code>pandas</code>, <code>BeautifulSoup</code>, y <code>Anaconda</code> para extracción de datos históricos mediante web scraping.</p>
    
    <h3>2. Conexión con BigQuery</h3>
    <p>Configura la VM para conectarse a BigQuery como base de datos principal. Asegúrate de usar una Cuenta de Servicio con los permisos adecuados para cargar los datos extraídos en un Dataset en BigQuery, para su almacenamiento y análisis.</p>
    
    <h3>3. Automatización de Ingesta de Datos</h3>
    <p>Configura <strong>Cloud Functions</strong> para actualizar automáticamente los datos cada día, manteniendo BigQuery siempre actualizado sin intervención manual.</p>
    
    <h3>4. Configuración de Visualización</h3>
    <p>Conecta BigQuery a <strong>Looker Studio</strong> y <strong>Power BI</strong> para visualizar y analizar los datos financieros en tiempo real y generar reportes históricos.</p>
    
    <h2>Visualización</h2>
    <ul>
        <li><strong>Power BI:</strong> Proporciona una plataforma para análisis avanzados y visualización de datos históricos.</li>
        <li><strong>Looker Studio:</strong> Ofrece gráficos y dashboards interactivos, ideales para reportes financieros.</li>
    </ul>
    
    <h2>Descripción General</h2>
    <p>Este proyecto integra herramientas en la nube para realizar un análisis financiero continuo y automatizado. La infraestructura en GCP asegura que los datos financieros estén siempre actualizados y listos para extraer insights valiosos.</p>
    
    <h2>Autor</h2>
    <p><strong>Nombre del autor</strong> - <em>Tu rol o descripción breve del autor</em></p>
</body>
</html>


## Descripción General

Este proyecto integra herramientas en la nube para realizar un análisis financiero continuo y automatizado. La infraestructura en GCP asegura que los datos financieros estén siempre actualizados y listos para extraer insights valiosos.


