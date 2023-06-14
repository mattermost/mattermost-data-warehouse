# STRIPE data

Consists of data coming in from Stripe via Stitch.

                      +------------+        +------------+
                      | Customers  |        |  Products  |
                      +------------+        +------------+
                            |                    /\
                            |                   /  \
                            |                  /    \
                            |                 /      \
                            |                /        \
                            |               /          \
                            |              /            \
                            |             /              \
                        +----------------+            +-------------------+
                        | Subscriptions  |------------| SubscriptionItems |
                        +----------------+            +-------------------+
                            |           /\
                            |          /  \
                            |         /    \
                            |        /      \
                            |       /        \
                            |      /          \
                            |     /            \
                      +----------+    +------------+ 
                      | Charges  |----|  Invoices  | 
                      +----------+    +------------+ 
