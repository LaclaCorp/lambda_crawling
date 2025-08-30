#############################################################################################
# @name: lambda_function
# @description
# - aws lambda 에서 환경 변수 설정
#############################################################################################

import os
import bt_proto
import bt_proto_past
import bt_toto
import bt_toto_past
import bt_toto_result


def lambda_handler(*args, **kwargs):
    # aws lambda 에서 설정한 환경 변수
    lambda_function_name = os.getenv('LAMBDA_FUNCTION_NAME')
    print("실행할 함수:", lambda_function_name)

    if lambda_function_name == "LAMBDA-PROTOS":
        crawling = bt_proto.lambda_handler()
    elif lambda_function_name == "LAMBDA-PROTOS-PAST":
        crawling = bt_proto_past.lambda_handler()
    elif lambda_function_name == "LAMBDA-TOTO":
        crawling = bt_toto.lambda_handler()
    elif lambda_function_name == "LAMBDA-TOTO-PAST":
        crawling = bt_toto_past.lambda_handler()
    elif lambda_function_name == "LAMBDA-TOTO-RESULT":
        crawling = bt_toto_result.lambda_handler()
    else:
        crawling = "환경 변수를 설정하세요."

    # 로그 출력 (CloudWatch에서 확인 가능)
    print("실행 결과:", crawling)

    return crawling if crawling is not None else "실행 완료"


if __name__ == "__main__":
    # os.environ["LAMBDA_FUNCTION_NAME"] = "LAMBDA-TOTO-RESULT"
    result = lambda_handler()
    print("로컬 실행 결과:", result)
    # print(result)
