class querycleaning:
    createtableas= (
        """
        begin;

        create table IF NOT EXISTS {Table_new} as
        SELECT distinct 
                {Columns}
        FROM {Table}
        LIMIT 1
        ;

        ALTER TABLE {Table_new}
            ADD CONSTRAINT {Table_new}_pk PRIMARY KEY ({set_column_pk});
        
        end;
        """
    )


    geolocation = (
        """
        with 
        unique_data as(
            select 
                distinct geolocation_id,
                geolocation_zip_code_prefix,
                geolocation_lat,
                geolocation_lng,
                unaccent(geolocation_city) geolocation_city,
                geolocation_state
            from integration.geolocation g 
        )
        select
            distinct geolocation_id,
            geolocation_zip_code_prefix,
            geolocation_lat,
            geolocation_lng,
            geolocation_city,
            geolocation_state
        from unique_data
        """
    )

    pcn = (
        """
        select distinct product_category_name,
                product_category_name_english
        from integration.product_category_name_translation
        """
    )

    customers = (
        """
        select distinct customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state
        from integration.customers
        """
    )

    sellers = (
        """
        select distinct seller_id, 
                seller_zip_code_prefix,
                seller_city,
                seller_state
        from integration.sellers s
        """
    )

    orders = (
        """
        select distinct order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date
        from integration.orders o 
        """
    )

    products = (
        """
        select distinct product_id,
                regexp_replace(product_category_name, '[[:punct:]]', ' ', 'g') product_category_name,
                product_name_lenght,
                product_description_lenght,
                product_photos_qty,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm
        from integration.products p
        """
    )

    order_reviews = (
        """
        select distinct review_order_id ,
                review_id,
                order_id,
                review_score,
                review_comment_title,
                review_comment_message,
                review_creation_date,
                review_answer_timestamp
	    from integration.order_reviews or2 
        """
    )

    order_items =(
        """
        select distinct order_product_id,
                order_id,
                order_item_id,
                product_id,
                seller_id,
                shipping_limit_date,
                price,
                freight_value
        from integration.order_items oi 

        """
    )

    order_payments = (
        """
        SELECT 
            distinct order_payment_id,
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value
        FROM integration.order_payments op 
        """
    )