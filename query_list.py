first_orders = """
    SELECT customer_id, MIN(created_at) AS first_job
    FROM job
    WHERE state='finished'
    OR state='assigned'
    OR (state='requested' AND is_custom=False)
    GROUP BY customer_id
    """

orders = """
        SELECT id, customer_id, latitiude AS latitude, longitude, recurring, job_type
        FROM job
        WHERE (state ='finished' OR state='assigned')
        AND created_at >= '2022-04-01'
        """
