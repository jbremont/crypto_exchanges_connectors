class OrderTracker(object):
    def __init__(self):
        self.internal_to_external_id = {}
        self.open_orders = {}
        self.pending_cancel = {}
