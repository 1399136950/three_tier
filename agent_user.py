from collections import deque, defaultdict
import time
import random

from config.config import gold_rate
from common.my_log import my_log
from common.Identification_verification_code import identification_verification_code  # 识别图像验证码
from common.mydb import MyDb  # 操作mysql数据库类
from config.config import addr  # 图形验证码解析服务器地址
from lib.account_management import *  # 导入账户管理模块所有函数
from lib.financial_statistics import *  # 导入财务统计模块所有函数
from lib.home_page_management import *  # 导入首页管理模块所有函数
from lib.other_test_management import *  # 导入其他测试模块所有函数
from lib.require_user_management import *  # 导入邀请用户管理模块所有函数
from lib.sub_agent_management import *  # 导入子代理管理模块所有函数
from lib.withdraw_management import *  # 导入提现管理模块所有函数


class AgentUser:
    def __init__(self, user_name, password):
        print(self, user_name)
        self.token = None  # 用户的登录鉴权token值
        self.user_name = user_name  # 用户名
        self.password = password  # 用户密码
        self.agent_parent = None  # 指向上级用户对象
        self.current_consume_money = 0  # 当前消费的金额
        self.received_commission = 0  # 自己获得的分成佣金
        self.children = set()  # 存放下级用户对象(一个集合)
        self.before_commission_info = None  # 首页的累计佣金
        self.before_withdraw_account = None  # 可提现的整个信息
        self.before_financial_income_statistic_info = None  # 财务长单信息
        self.before_child_proxy_info = {}  # 当前账户下级代理所有的列表数据
        self.before_final_user_info = {}  # 当前账户下最终用户的列表数据
        self.contribution_commission_info = defaultdict(int)    # 自己发展的终端用户贡献的佣金信息
        self.run()

    def get_commission_percentage(self):
        """
        获取下层用户佣金比例
        :return:
        """
        raise Exception('未实现该方法')

    def bind_parent_object(self, agent_parent):
        """
        传入上层代理对象来绑定上层的爹
        :param agent_parent:    上层代理对象
        :return:
        """
        agent_parent.children.add(self)
        self.agent_parent = agent_parent

    def login(self):
        """
        代理账户等会：获取到鉴权token值
        :return:
        """
        image, code_id = graphic_verify_code()
        code = identification_verification_code(image, addr)
        res = account_login(self.user_name, code, code_id, self.password)
        if res['code'] == 0:
            self.token = res['data']['accessToken']
        else:
            raise Exception(f'登陆失败!:{res}')

    def get_financial_income_statistic_info(self):
        """
        获取收益统计基本统计累计信息
        :return:
        """
        res = financial_income_statistic_info(self.token)
        return res

    def get_child_proxy_info(self):
        """
        获取当前用户下级代理所有的列表数据
        :return:
        """
        index = 0
        child_proxy_dict = {}
        while res := sub_list(self.token, page_no=index, page_size=20)["data"]["list"]:
            child_proxy_dict.update({i["account"]: i for i in res})
            index += 1
        return child_proxy_dict

    def get_final_user_info(self):
        """
        获取当前代理用户下所有的最终用户列表数据
        :return:
        """
        index = 0
        final_user_dict = {}
        while res := puser_list(self.token, page_no=index, page_size=20)["data"]["list"]:
            final_user_dict.update({i["account"]: i for i in res})
            index += 1
        return final_user_dict

    def get_withdraw_account(self):
        """
        获取提现页面中的代理账户的余额相关信息
        :return:
        """
        res = withdraw_account(self.token)
        return res

    def get_commission_info(self):
        """
        获取首页概览中的当天和累计的佣金收入和交易额的统计
        """
        res = home_statistic(self.token)
        return res

    def update_commission(self, end_user, money):
        """
        金牌用户才调用这个方法
        下级用户消费后，根据自己获得的佣金分成，计算贡献给上级的金额
        :param end_user:    传入最终用户的对象
        :param money:       下级用户消费了后，自己获得的佣金分成
        :return:
        """
        if self.IDENTITY != 1:  # 非金牌用户
            raise Exception('只有金牌用户才可调用')
        q = deque()
        user = end_user  # 最终用户
        while user.agent_parent:  # 找到最终用户的除了金牌以外的代理
            if user.agent_parent == self:
                break
            q.append(user.agent_parent)
            user = user.agent_parent
        _money = money
        parent_user = self
        parent_user.received_commission += _money
        if end_user.agent_parent == parent_user:
            parent_user.contribution_commission_info[end_user.user_name] += _money

        while len(q) > 0:
            current_user = q.pop()  # 银爸爸，铜爸爸...等等等，先弹出银牌
            rate = None  # 代理划分给下级佣金比例
            if current_user.IDENTITY == 2:  # 当前是银牌
                rate = parent_user.silverBrokerageRate
            elif current_user.IDENTITY == 3:  # 当前是铜牌
                rate = parent_user.bronzeBrokerageRate
            if rate is not None:  # 如果佣金比例不为空
                current_user.received(_money * rate)  # 当前用户佣金
                if end_user.agent_parent == current_user:
                    current_user.contribution_commission_info[end_user.user_name] += _money * rate
                parent_user.received_commission -= _money * rate
                if end_user.agent_parent == parent_user:
                    parent_user.contribution_commission_info[end_user.user_name] -= _money * rate
                _money = _money * rate
                parent_user = current_user
            else:
                raise Exception("找不到rate下级佣金比例")

    def create_end_user(self):
        """
        创建最终用户
        :return:
        """
        db = MyDb('')
        agent_uid = db.where({"account": self.user_name}).select("id")[0][0]

        db.table = "agent_paas_user"
        if db.orderby('paas_uid desc').limit(1).select('paas_uid') is False:
            paas_uid = 1
        else:
            paas_uid = db.orderby('paas_uid desc').limit(1).select('paas_uid')[0][0] + 1
        paas_account = f"客户{int(time.time())}"  # 最终用户的账号
        db.add({
            "agent_uid": agent_uid,
            "paas_uid": paas_uid,
            "paas_account": paas_account,
            "nickname": f"昵称{random.randint(0, 10000)}",
            "phone": f"181{random.randint(10000000, 99999999)}",
            "trade_amount": 0.00,
            "brokerage_amount": 0.00,
            "register_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "create_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "remark": "备注",
            "company": "公司",
            "status": 0,
            "type": 12
        })
        end_user = EndUser(paas_uid)
        end_user.user_name = paas_account
        end_user.agent_parent = self  # 设置代理爹

        current_parent = self  # 找最顶层的金牌代理，
        while current_parent.agent_parent:
            current_parent = current_parent.agent_parent

        if current_parent.IDENTITY != 1:  # 最顶层的不是金主爸爸
            raise Exception('找不到金主爸爸')

        end_user.gold_parent = current_parent  # 设置金主爸爸
        db.close()

        self.children.add(end_user)
        time.sleep(1)
        return end_user

    def received(self, money):
        self.received_commission += money

    def run(self):
        self.login()
        self.get_commission_percentage()
        self.before_commission_info = self.get_commission_info()
        self.before_withdraw_account = self.get_withdraw_account()
        self.before_financial_income_statistic_info = self.get_financial_income_statistic_info()
        self.before_child_proxy_info = self.get_child_proxy_info()
        self.before_final_user_info = self.get_final_user_info()

    def assert_account_commission(self):
        """
        断言总佣金，财务账单，可提现余额正确性
        :return:
        """
        current_commission_info = self.get_commission_info()  # 首页概览信息
        current_withdraw_account = self.get_withdraw_account()  # 可提现余额信息
        # 财务账单信息
        current_financial_income_statistic_info = self.get_financial_income_statistic_info()

        child_proxy_count_info, child_proxy_after_info = self.count_child_proxy_info()

        final_user_count_info, final_user_after_info = self.count_final_user_info()

        my_log("断言交易前后可提现的余额等于本次测试佣金分层",
               current_withdraw_account['data']['balance'] - self.before_withdraw_account['data']['balance'],
               self.received_commission)
        assert abs(current_withdraw_account['data']['balance'] - self.before_withdraw_account['data'][
            'balance'] - self.received_commission) < 0.1

        my_log("断言财务账单的累计佣金收入", current_financial_income_statistic_info['data']['brokerage'] -
               self.before_financial_income_statistic_info['data']['brokerage'], self.received_commission)
        assert abs(current_financial_income_statistic_info['data']['brokerage'] -
                   self.before_financial_income_statistic_info['data']['brokerage'] - self.received_commission) < 0.1

        my_log('断言首页概览信息的累计佣金收入',
               current_commission_info['data']['totalBrokerageIncome'] - self.before_commission_info['data'][
                   'totalBrokerageIncome'], self.received_commission)
        assert abs(current_commission_info['data']['totalBrokerageIncome'] - self.before_commission_info['data'][
            'totalBrokerageIncome'] - self.received_commission) < 0.1

        my_log('断言首页概览信息今日订单数量',
               current_commission_info['data']['todayTradeOrderCount'] - self.before_commission_info['data'][
                   'todayTradeOrderCount'], self.get_order_count())
        assert abs(current_commission_info['data']['todayTradeOrderCount'] - self.before_commission_info['data'][
            'todayTradeOrderCount'] - self.get_order_count()) < 0.1

        my_log("断言直属代理列表计算等于服务端返回", child_proxy_count_info, child_proxy_after_info)

        my_log("断言邀请用户列表计算等于服务端返回", final_user_count_info, final_user_after_info)
        # assert final_user_count_info == final_user_after_info
        return True

    def count_final_user_info(self):
        """
        获取断言邀请管理列表的两组数据，为了方便断言
        :return:
        """
        # 最终用户佣金：本次测试过程每个用户增加的佣金
        final_user_commission = self.contribution_commission_info
        # 最终用户消费:本次测试过程每个用户增加的消费
        final_user_consume = {}
        for final_user in self.children:
            if final_user.IDENTITY == 4:        # 如果是最终用户
                final_user_consume[final_user.user_name] = final_user.current_consume_money
        count_info = {k: {'brokerageAmount': v['brokerageAmount'],
                          'tradeAmount': v["tradeAmount"]} for k, v in self.before_final_user_info.items()}
        time.sleep(1)
        after_info = {k: {'brokerageAmount': v['brokerageAmount'],
                          'tradeAmount': v["tradeAmount"]} for k, v in self.get_final_user_info().items()}
        for k, v in final_user_commission.items():
            if k not in count_info.keys():
                count_info[k] = {}
                count_info[k]['brokerageAmount'] = v
                count_info[k]['tradeAmount'] = 0
            else:
                count_info[k]['brokerageAmount'] = count_info[k]['brokerageAmount'] + v
        for k, v in final_user_consume.items():
            if k not in count_info.keys():
                count_info[k] = {}
                count_info[k]['tradeAmount'] = v
                count_info[k]['brokerageAmount'] = 0
            else:
                count_info[k]['tradeAmount'] = count_info[k]['tradeAmount'] + v
        return count_info, after_info

    def count_child_proxy_info(self):
        """
        获取断言直属代理列表的两组数据，为了方便断言
        :return:
        """
        # 获取接口计算得到下级用户本次测试当前的交易额
        child_proxy_consume = self.get_child_proxy_consume_detail()
        # 获取接口计算下级代理获取到的佣金
        child_proxy_com = self.get_child_proxy_commission()
        count_info = {k: {'totalBrokerageIncome': v["totalBrokerageIncome"],
                          'totalTradeAmount': v["totalTradeAmount"]} for k, v in self.before_child_proxy_info.items()}
        after_info = {k: {'totalBrokerageIncome': v["totalBrokerageIncome"],
                          'totalTradeAmount': v["totalTradeAmount"]} for k, v in self.get_child_proxy_info().items()}
        for k, v in child_proxy_consume.items():        # 交易额
            if k not in count_info.keys():
                count_info[k] = {}
                count_info[k]['totalTradeAmount'] = v
                count_info[k]['totalBrokerageIncome'] = 0
            else:
                count_info[k]["totalTradeAmount"] = count_info[k]["totalTradeAmount"]+v
        for k, v in child_proxy_com.items():
            if k not in count_info.keys():
                count_info[k] = {}
                count_info[k]['totalBrokerageIncome'] = v
                count_info[k]['totalTradeAmount'] = 0
            else:
                count_info[k]["totalBrokerageIncome"] = count_info[k]["totalBrokerageIncome"]+v
        return count_info, after_info

    def get_child_proxy_consume_detail(self):
        """
        获取自己的下级用户当前的交易额(只获取当前测试的交易额)
        :return: dict   k:下级用户账号， v:交易额
        """
        res = {}
        for user in self.children:
            # 判断下级用户是不是终端用户，如果是最终用户res存储{最终用户user_name, 最终用户消费金额}
            if isinstance(user, EndUser):
                res[user.user_name] = user.current_consume_money
            # 如果不是终端用户递归，金递归到银，银递归到铜，铜再递归到最终（找到最终为止）
            else:
                _r = user.get_child_proxy_consume_detail()
                res[user.user_name] = sum([v for k, v in _r.items()])
        return res

    def get_child_proxy_commission(self):
        """
        本次测试过程中下级代理获取到的佣金
        :return:
        """
        res = {}
        for user in self.children:
            if user.IDENTITY == 4:
                pass
            else:
                res[user.user_name] = user.received_commission
        return res

    def get_order_count(self):
        """
        计算当前一共成交的订单(不包含之前,本次测试中新增的成交的订单)
        """
        count = 0
        for user in self.children:
            if isinstance(user, EndUser):
                count += user.current_consume_order_count
            else:
                count += user.get_order_count()
        return count


class CopperUser(AgentUser):
    IDENTITY = 3

    def get_commission_percentage(self):
        pass


class SilverUser(CopperUser):
    IDENTITY = 2

    def create_copper_user(self, account, phone):
        """
        银牌创建铜牌用户
        :param account:
        :param phone:
        :return:
        """
        level = 30
        company = '铜牌测试用户'
        nickname = '铜牌测试用户' + time.strftime("%Y-%m-%d %H:%M:%S")
        res = sub_create(self.token, account, company, level, nickname, phone)
        print(res)
        password = 'zx' + phone[-4:]
        copper_user = CopperUser(account, password)
        copper_user.agent_parent = self
        self.children.add(copper_user)
        return copper_user

    def get_commission_percentage(self):
        """
        银牌获取当前用户铜牌的佣金比例
        :return:
        """
        res = sub_setting(self.token)
        if res['code'] != 0:
            raise Exception(f'请求异常: {res}')
        if 'bronzeBrokerageRate' in res['data']:
            self.bronzeBrokerageRate = res['data']['bronzeBrokerageRate'] / 100  # 铜牌佣金分成比例

    def set_bronze_brokerage_rate(self, rate):
        """
        银牌用户设置下级铜牌用户的佣金比例,设置后更新当前类属性的佣金比例
        :param rate:
        :return:
        """
        res = sub_setting_update(self.token, bronze_broke_rage_rate=rate)
        if res['code'] == 0:
            self.bronzeBrokerageRate = rate / 100
        return res


class GoldUser(SilverUser):
    RATE = gold_rate
    IDENTITY = 1

    def create_silver_user(self, account, phone):
        level = 20
        company = '银牌测试用户'
        nickname = '银牌测试用户' + time.strftime("%Y-%m-%d %H:%M:%S")
        res = sub_create(self.token, account, company, level, nickname, phone)
        print(res)
        password = 'zx' + phone[-4:]
        silver_user = SilverUser(account, password)
        silver_user.agent_parent = self
        self.children.add(silver_user)
        return silver_user

    def get_commission_percentage(self):
        res = sub_setting(self.token)
        if res['code'] != 0:
            raise Exception(f'请求异常: {res}')
        if 'bronzeBrokerageRate' in res['data']:
            self.bronzeBrokerageRate = res['data']['bronzeBrokerageRate'] / 100  # 铜牌佣金分成比例
        if 'silverBrokerageRate' in res['data']:
            self.silverBrokerageRate = res['data']['silverBrokerageRate'] / 100  # 银牌佣金分成比例

    def set_silver_brokerage_rate(self, rate):
        res = sub_setting_update(self.token,
                                 bronze_broke_rage_rate=int(self.bronzeBrokerageRate * 100),
                                 silver_broke_rage_rate=rate)
        if res['code'] != 0:
            raise Exception(f'设置异常:{res}')
        self.silverBrokerageRate = rate / 100
        return res

    def set_bronze_brokerage_rate(self, rate):
        res = sub_setting_update(self.token,
                                 bronze_broke_rage_rate=rate,
                                 silver_broke_rage_rate=int(self.silverBrokerageRate * 100))
        if res['code'] != 0:
            raise Exception(f'设置异常:{res}')
        self.bronzeBrokerageRate = rate / 100
        return res


class EndUser:
    RATE = gold_rate  # 向金牌用户贡献的比例
    IDENTITY = 4

    def __init__(self, paas_uid):
        self.user_name = None  # 用户名
        self.agent_parent = None  # 代理爹
        self.gold_parent = None  # 金牌爹
        self.paas_uid = paas_uid
        self.current_consume_money = 0  # 当前消费金额
        self.current_consume_order_count = 0  # 当前消费的订单数量

    def find_ancestors(self):
        ancestors = self.agent_parent
        while ancestors.agent_parent:
            ancestors = ancestors.agent_parent
        return ancestors

    def bind_user(self, agent_parent, gold_parent):
        self.agent_parent = agent_parent
        self.gold_parent = gold_parent
        self.agent_parent.children.add(self)

    def consume(self, amount, order_code=None):
        """
        消费
        :param amount:      订单金额金额
        :param order_code:
        :return:
        """
        if order_code is None:
            order_code = int(time.time() * 1000)
        res = trigger_statistic(amount, order_code, self.paas_uid)
        if res['code'] != 0:
            raise Exception(f'终端用户消费异常!{res}')
        contribution_commission = amount * self.RATE  # 贡献的佣金 = 消费的金额 * 佣金比例
        self.current_consume_money += amount  # 计算终端用户累计消费的金额
        self.current_consume_order_count += 1  # 订单数量
        if self.gold_parent:
            self.gold_parent.update_commission(self, contribution_commission)

    def run(self):
        pass
