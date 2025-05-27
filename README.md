# **About Project**

Since it is difficult for teams to make data-driven decisions, understand data and table relationships and create reports through OLTP, it was decided to build a data warehouse using dimensional data modeling for the marketing, sales and customer relations departments that applied to the data team.

For this purpose, the following gains will be achieved:
        
    - **Müşterilerin satın alma davranışları**,
    - **Çalışanların ve dükkanların performansları**,
    - **Filmlerin tercih edilmesinin veriye dayalı anlaşılması**.


- **Veri Modelleme Aşamaları**

    - Conceptual Modelling:

    ![conceptual_model](modelling_images/conceptual_model.png)
        
    - Logical Modelling:

    ![logical_model](modelling_images/logical_model.png)

    - Physical Modelling:

    ![physical_model](modelling_images/physical_model.png)

- **ELT Process**

    - OLTP → Staging → DWH

- **Technologies Used**

    - Python, Airflow, BigQuery, PostgreSQL