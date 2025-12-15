print("MADHAVA")

from td_utils import tdquery

def sd():
    try:
        dg=tdquery("SELECBDFBZT 'GOVINDA'DFGBDZFBDFB;")
        return_code=0

    except Exception as e:
        dg=e
        print(e,type(e))
        return_code=1

    return str(dg) ,return_code

print(sd()[1])
