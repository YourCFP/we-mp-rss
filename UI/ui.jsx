import React from 'react';
import { motion } from 'framer-motion';
import { Clock, Newspaper, Megaphone, Gavel, Shield, Eye, Send, Factory, Leaf, Ship, Scale, Heart, Bus, Calendar, Brain, Wifi, Droplets } from 'lucide-react';

const PublicAccountPlatform = () => {
  // 公众号数据
  const accounts = [
    {
      name: '湖南日报',
      desc: '《湖南日报》是中共湖南省委机关报，是湖南省最具权威性、发行量最大的综合性大报。',
      createTime: '2024-03-22',
      updateTime: '2025-05-13',
      icon: <Newspaper size={40} />
    },
    {
      name: '长沙晚报',
      desc: '在这里，了解长沙！在这里，爱上长沙！',
      createTime: '2024-03-22',
      updateTime: '2025-05-12',
      icon: <Newspaper size={40} />
    },
    {
      name: '长沙市场监管',
      desc: '长沙市市场监督管理局官方公众号平台',
      createTime: '2024-03-26',
      updateTime: '2025-05-11',
      icon: <Scale size={40} />
    },
    {
      name: '微开福',
      desc: '展示区域形象，发布政务信息，提供便民服务。',
      createTime: '2025-05-09',
      updateTime: '2025-05-12',
      icon: <Megaphone size={40} />
    },
    {
      name: '清廉长沙',
      desc: '长沙市纪委市监委官微，廉政宣传教育，接受"四风"举报。',
      createTime: '2024-03-22',
      updateTime: '2025-05-11',
      icon: <Gavel size={40} />
    },
    {
      name: '长沙警事',
      desc: '长沙市公安局警务信息发布平台',
      createTime: '2024-03-22',
      updateTime: '2025-05-11',
      icon: <Shield size={40} />
    },
    {
      name: '文明长沙',
      desc: '传播正能量',
      createTime: '2024-04-29',
      updateTime: '2025-05-11',
      icon: <Heart size={40} />
    },
    {
      name: '长沙观察',
      desc: '长沙电视台新闻频道新媒体官方平台，关注热点新闻及综合资讯。',
      createTime: '2024-03-22',
      updateTime: '2025-05-11',
      icon: <Eye size={40} />
    },
    {
      name: '长沙发布',
      desc: '传递政务信息，提供服务资讯，关注百姓关切——长沙发布随时为您守候。',
      createTime: '2024-04-29',
      updateTime: '2025-05-12',
      icon: <Send size={40} />
    },
    {
      name: '新华社',
      desc: '新华通讯社官方账号。硬新闻、暖故事、好朋友。',
      createTime: '2024-03-22',
      updateTime: '2025-05-11',
      icon: <Newspaper size={40} />
    },
    {
      name: '长沙生态环境',
      desc: '宣传环保政策法规、发布长沙环保工作动态、普及环境科学知识、聚焦环保热点问题，倡导绿色、低碳的发展和生活方式！',
      createTime: '2024-04-29',
      updateTime: '2025-05-11',
      icon: <Leaf size={40} />
    },
    {
      name: '雨花市场监管',
      desc: '长沙市雨花区市场监督管理局官方公众号平台',
      createTime: '2024-03-26',
      updateTime: '2025-05-11',
      icon: <Scale size={40} />
    },
    {
      name: '开福市场监管',
      desc: '宣传市场监管知识、传达各级文件会议精神、解读国家相关政策',
      createTime: '2024-03-26',
      updateTime: '2025-05-11',
      icon: <Scale size={40} />
    },
    {
      name: '宁乡市场监督',
      desc: '宁乡市市场监督管理局信息发布平台',
      createTime: '2024-03-26',
      updateTime: '2025-05-11',
      icon: <Scale size={40} />
    },
    {
      name: '长沙县市场监管',
      desc: '了解长沙县市场监督管理局工作动态，并提供相关业务咨询服务。',
      createTime: '2024-03-26',
      updateTime: '2025-05-11',
      icon: <Scale size={40} />
    },
    {
      name: '长沙城管',
      desc: '"长沙城管"秉承"城管为民"服务理念，向公众提供城市管理动态、政策法规、投诉建议等服务，助力品质长沙建设，提高市民参与度与获得感。',
      createTime: '2024-04-29',
      updateTime: '2025-05-11',
      icon: <Factory size={40} />
    },
    {
      name: '长沙慈善',
      desc: '弘扬慈善文化，传递慈善正能量！',
      createTime: '2025-03-14',
      updateTime: '2025-05-12',
      icon: <Heart size={40} />
    },
    {
      name: '长沙交通运输',
      desc: '服务百姓出行，传播交通声音！',
      createTime: '2024-04-29',
      updateTime: '2025-05-12',
      icon: <Bus size={40} />
    },
    {
      name: '长沙会展',
      desc: '聚焦会展行业，传播会展资讯，链接会展服务，发展会展经济。',
      createTime: '2024-04-29',
      updateTime: '2025-05-11',
      icon: <Calendar size={40} />
    },
    {
      name: '思想长沙',
      desc: '长沙市社会科学界联合会（长沙社会科学院）官方账号',
      createTime: '2024-04-29',
      updateTime: '2025-05-11',
      icon: <Brain size={40} />
    },
    {
      name: '网信长沙',
      desc: '解读网信政策法规，展示长沙网信动态，交流网信工作经验，推进网络文明建设，走好网上群众路线。',
      createTime: '2024-04-29',
      updateTime: '2025-05-12',
      icon: <Wifi size={40} />
    },
    {
      name: '长沙水运',
      desc: '长沙市水运事务中心（长沙市船舶检验中心）官方账号。',
      createTime: '2024-04-29',
      updateTime: '2025-05-12',
      icon: <Ship size={40} />
    },
    {
      name: '天心市场监管',
      desc: '长沙市天心区市场监督管理局官方公众号平台',
      createTime: '2024-10-31',
      updateTime: '2025-05-12',
      icon: <Scale size={40} />
    }
  ];

  return (
    <div className="min-h-screen bg-[#F5F7FA] text-[#212121]">
      {/* 标题区 */}
      <header className="h-20 bg-white flex items-center justify-center shadow-sm">
        <div className="container mx-auto px-4">
          <h1 className="text-2xl md:text-3xl font-bold text-[#1A237E]">长沙政务公众号信息平台</h1>
          <div className="h-px bg-[#E0E0E0] mt-2"></div>
        </div>
      </header>

      {/* 内容区 */}
      <main className="container mx-auto px-4 py-8">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {accounts?.map((account, index) => (
            <motion.div
              key={index}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.3, delay: index * 0.05 }}
              whileHover={{ y: -5, boxShadow: '0 4px 16px rgba(0,0,0,0.15)' }}
              className="bg-white rounded-lg shadow-md overflow-hidden cursor-pointer min-h-[180px]"
            >
              <div className="p-5">
                <div className="flex items-center mb-4">
                  <div className="mr-3">
                    {account?.icon}
                  </div>
                  <h3 className="text-lg font-bold text-[#1A237E]">{account?.name}</h3>
                </div>
                <p className="text-sm text-[#616161] mb-4 line-clamp-2">{account?.desc}</p>
                <div className="flex flex-wrap gap-2">
                  <div className="flex items-center text-xs text-[#9E9E9E] bg-[#E3F2FD] px-2 py-1 rounded">
                    <Clock size={12} className="mr-1" />
                    <span>创建 {account?.createTime}</span>
                  </div>
                  <div className="flex items-center text-xs text-[#9E9E9E] bg-[#EEEEEE] px-2 py-1 rounded">
                    <Clock size={12} className="mr-1" />
                    <span>更新 {account?.updateTime}</span>
                  </div>
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </main>

      {/* 页脚区 */}
      <footer className="bg-[#FAFAFA] py-5">
        <div className="container mx-auto px-4 text-center text-xs text-[#757575]">
          <p>created by <a href="https://space.coze.cn" className="text-[#2196F3] hover:text-[#0D47A1]">coze space</a></p>
          <p>页面内容均由 AI 生成，仅供参考</p>
        </div>
      </footer>
    </div>
  );
};

export default PublicAccountPlatform;