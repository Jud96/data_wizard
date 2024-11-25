from LinkedService.linked_service import LinkedService

class LinkedServiceToDB(LinkedService):
    def __init__(self, db_name, user, password, host, port):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        pass