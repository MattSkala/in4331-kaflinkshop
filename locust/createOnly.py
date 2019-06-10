from locust import Locust, HttpLocust, TaskSet, task
import json
import random
import resource
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (999999, 999999))
setUsers = set()
setOrders = set()
setItems = set()

class Webshop(TaskSet):
    @task(1)
    def createUser(self):
        response = self.client.post("/users/create")
        data = json.loads(response.text)
        user_id = data['result']['params']['user_id']
        setUsers.add(user_id)
    @task(1)
    def createItem(self):
        response = self.client.post("/stock/item/create", name="/stock/item/create")
        data = json.loads(response.text)
        item_id = data['result']['params']['item_id']
        setItems.add(item_id)

class WebsiteUser(HttpLocust):
    task_set = Webshop
    min_wait = 1000
    max_wait = 9000
