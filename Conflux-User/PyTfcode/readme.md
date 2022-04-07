
# Python -- Tensorflow usage logger

## 2021-06-20

With TF C APIs, it seems almost impossible to avoid kinds of buffer allocating and metadata loading actions.

To reduce inference software overhead, I deicide to exploit TF lite library later.

## 2021-06-29

Using TF lite, one inference takes <=4us, which may be further reduced by simplifying the model.
