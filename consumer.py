import pika, json


params = pika.URLParameters(
    "amqps://bchoukaa:nmy6nGxY3nZaJDKY0s2zEwzjMr4G36Pw@cow.rmq2.cloudamqp.com/bchoukaa"
)

connection = pika.BlockingConnection(params)

channel = connection.channel()

channel.queue_declare(queue="main")


def callback(ch, method, properties, body):

    print("Received in mainddd")
    data = json.loads(body)
    print(data)

    if properties.content_type == "product_created":
        import app

        with app.app.app_context():
            product = app.Product(
                id=data["id"], title=data["title"], image=data["image"], likes=0
            )
            app.db.session.add(product)
            app.db.session.commit()
            print("Product Created")

    elif properties.content_type == "product_updated":
        import app

        with app.app.app_context():
            product = app.Product.query.get(data["id"])
            product.title = data["title"]
            product.image = data["image"]
            app.db.session.commit()
            print("Product Updated")

    elif properties.content_type == "product_deleted":
        import app

        with app.app.app_context():
            product = app.Product.query.get(data)
            app.db.session.delete(product)
            app.db.session.commit()
            print("Product Deleted")


channel.basic_consume(queue="main", on_message_callback=callback, auto_ack=True)

print("Started Consuming")

channel.start_consuming()

channel.close()
