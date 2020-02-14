class MyOwnHook(BaseHook):

    def __init__(self, conn_id):
        # create connection instance herereturn self._conndef do_stuff(self, arg1, arg2, **kwargs):session = self.get_conn()session.do_stuff(# perform some action...
        super().__init__(source=None)self._conn_id = conn_idself._conn = Nonedef get_conn(self): if self._connis None: self._conn = object()
