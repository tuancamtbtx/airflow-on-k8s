SELECT CAST(sku AS STRING) sku, product_fraud_score_new score
      FROM `dwh.product.vw_product_fraud_score_update`
      WHERE sku IS NOT NULL