<script setup lang="ts">
import { ref, reactive, onMounted, computed, h } from 'vue'
import { 
  createNode, 
  listNodes, 
  updateNode,
  deleteNode,
  generateNodeCredentials,
  testNodeConnection,
  getSyncLogs,
  getAllocations,
  dispatchTask
} from '@/api/cascade'
import type { CascadeNode, CreateNodeRequest, UpdateNodeRequest, SyncLog, TaskAllocation } from '@/api/cascade'
import { Modal, Message } from '@arco-design/web-vue'
import { IconCopy, IconDelete, IconEdit, IconRefresh, IconLink, IconCheckCircle, IconCloseCircle, IconThunderbolt, IconQuestionCircle } from '@arco-design/web-vue/es/icon'

const columns = [
  { title: '节点名称', dataIndex: 'name' },
  { title: '节点类型', slotName: 'node_type' },
  { title: 'API地址', dataIndex: 'api_url', ellipsis: true },
  { title: 'API Key', dataIndex: 'api_key', width: '15%', ellipsis: true },
  { title: '状态', slotName: 'status' },
  { title: '最后心跳', slotName: 'last_heartbeat_at' },
  { title: '最后同步', slotName: 'last_sync_at' },
  { title: '操作', slotName: 'action', width: '15%' }
]

const logColumns = [
  { title: '操作类型', dataIndex: 'operation' },
  { title: '方向', slotName: 'direction' },
  { title: '状态', slotName: 'status' },
  { title: '数据量', dataIndex: 'data_count' },
  { title: '开始时间', dataIndex: 'started_at' },
  { title: '完成时间', dataIndex: 'completed_at' }
]

const allocationColumns = [
  { title: '分配ID', dataIndex: 'allocation_id', ellipsis: true, width: 120 },
  { title: '任务ID', dataIndex: 'task_id', ellipsis: true, width: 120 },
  { title: '节点名称', dataIndex: 'node_name' },
  { title: '公众号数', dataIndex: 'feed_ids', slotName: 'feed_count' },
  { title: '状态', slotName: 'allocation_status' },
  { title: '创建时间', slotName: 'created_at' },
  { title: '更新时间', slotName: 'updated_at' }
]

const nodeList = ref<CascadeNode[]>([])
const logList = ref<SyncLog[]>([])
const allocationList = ref<TaskAllocation[]>([])
const totalLogs = ref(0)
const totalAllocations = ref(0)
const loading = ref(false)
const logLoading = ref(false)
const allocationLoading = ref(false)
const dispatching = ref(false)
const error = ref('')
const visible = ref(false)
const modalTitle = ref('创建节点')
const selectedNode = ref<CascadeNode | null>(null)
const showCredentialModal = ref(false)
const credentials = ref<{ api_key: string; api_secret: string }>({ api_key: '', api_secret: '' })
const activeTab = ref('nodes')

const form = reactive({
  node_type: 1,
  name: '',
  description: '',
  api_url: ''
})

const logPagination = reactive({
  limit: 20,
  offset: 0
})

const fetchNodes = async () => {
  try {
    loading.value = true
    error.value = ''
    const res = await listNodes()
    nodeList.value = res || []
  } catch (err) {
    error.value = err instanceof Error ? err.message : '获取节点列表失败'
  } finally {
    loading.value = false
  }
}

const fetchLogs = async () => {
  try {
    logLoading.value = true
    const res = await getSyncLogs({
      limit: logPagination.limit,
      offset: logPagination.offset
    })
    logList.value = res?.list || []
    totalLogs.value = res?.total || 0
  } catch (err) {
    console.error('获取同步日志失败:', err)
  } finally {
    logLoading.value = false
  }
}

const fetchAllocations = async () => {
  try {
    allocationLoading.value = true
    const res = await getAllocations({ limit: 50 })
    allocationList.value = res?.list || []
    totalAllocations.value = res?.total || 0
  } catch (err) {
    console.error('获取任务分配失败:', err)
  } finally {
    allocationLoading.value = false
  }
}

const handleDispatchTask = async () => {
  try {
    dispatching.value = true
    await dispatchTask()
    Message.success('任务分发成功')
    fetchAllocations()
  } catch (err) {
    Message.error('任务分发失败: ' + (err instanceof Error ? err.message : '未知错误'))
  } finally {
    dispatching.value = false
  }
}

const showCreateModal = () => {
  modalTitle.value = '创建节点'
  selectedNode.value = null
  Object.assign(form, {
    node_type: 1,
    name: '',
    description: '',
    api_url: ''
  })
  visible.value = true
}

const editNode = (record: CascadeNode) => {
  modalTitle.value = '编辑节点'
  selectedNode.value = record
  Object.assign(form, {
    node_type: record.node_type,
    name: record.name,
    description: record.description || '',
    api_url: record.api_url || ''
  })
  visible.value = true
}

const handleSubmit = async () => {
  try {
    if (!form.name.trim()) {
      Message.error('请输入节点名称')
      return
    }

    if (modalTitle.value === '创建节点') {
      const data: CreateNodeRequest = {
        node_type: form.node_type,
        name: form.name,
        description: form.description,
        api_url: form.api_url || undefined
      }
      await createNode(data)
      Message.success('创建成功')
    } else if (selectedNode.value) {
      const data: UpdateNodeRequest = {
        name: form.name,
        description: form.description,
        api_url: form.api_url || undefined
      }
      await updateNode(selectedNode.value.id, data)
      Message.success('更新成功')
    }
    
    visible.value = false
    fetchNodes()
  } catch (err) {
    error.value = err instanceof Error ? err.message : '操作失败'
    Message.error(error.value)
  }
}

const handleDelete = (record: CascadeNode) => {
  Modal.confirm({
    title: '删除节点',
    content: `确定要删除 "${record.name}" 吗？此操作不可恢复。`,
    okText: '删除',
    cancelText: '取消',
    okButtonProps: { status: 'danger' },
    onOk: async () => {
      try {
        await deleteNode(record.id)
        Message.success('已删除')
        fetchNodes()
      } catch (err) {
        error.value = err instanceof Error ? err.message : '删除失败'
        Message.error(error.value)
      }
    }
  })
}

const handleGenerateCredentials = async (record: CascadeNode) => {
  Modal.confirm({
    title: '生成凭证',
    content: `为节点 "${record.name}" 生成新的 API 凭证。旧凭证将失效，确定继续吗？`,
    okText: '生成',
    cancelText: '取消',
    okButtonProps: { status: 'warning' },
    onOk: async () => {
      try {
        const res = await generateNodeCredentials(record.id)
        credentials.value = res
        showCredentialModal.value = true
      } catch (err) {
        error.value = err instanceof Error ? err.message : '生成凭证失败'
        Message.error(error.value)
      }
    }
  })
}

const handleTestConnection = async (record: CascadeNode) => {
  Message.loading({ content: '测试连接中...', id: 'test-connection' })
  try {
    await testNodeConnection(record.id)
    Message.success({ content: '连接测试成功', id: 'test-connection' })
  } catch (err) {
    Message.error({ content: '连接测试失败: ' + (err instanceof Error ? err.message : '未知错误'), id: 'test-connection' })
  }
}

const copyToClipboard = (text: string) => {
  navigator.clipboard.writeText(text).then(() => {
    Message.success('已复制到剪贴板')
  }).catch(() => {
    Message.error('复制失败')
  })
}

const copyCredentials = () => {
  const text = `API Key: ${credentials.value.api_key}\nAPI Secret: ${credentials.value.api_secret}`
  copyToClipboard(text)
  Message.success('已复制到剪贴板')
}

const formatDate = (dateStr: string | undefined) => {
  if (!dateStr) return '-'
  try {
    const date = new Date(dateStr)
    return date.toLocaleString('zh-CN')
  } catch {
    return '-'
  }
}

const getNodeTypeText = (nodeType: number) => {
  return nodeType === 0 ? '父节点' : '子节点'
}

const getNodeTypeColor = (nodeType: number) => {
  return nodeType === 0 ? 'blue' : 'green'
}

const getNodeStatusColor = (status: number) => {
  return status === 1 ? 'green' : 'red'
}

const getNodeStatusText = (status: number) => {
  return status === 1 ? '在线' : '离线'
}

const getDirectionText = (direction: string) => {
  const map: Record<string, string> = {
    pull: '拉取',
    push: '推送'
  }
  return map[direction] || direction
}

const getDirectionColor = (direction: string) => {
  return direction === 'pull' ? 'blue' : 'green'
}

const getLogStatusColor = (status: number) => {
  if (status === 1) return 'green'
  if (status === -1) return 'red'
  return 'gray'
}

const getLogStatusText = (status: number) => {
  if (status === 1) return '成功'
  if (status === -1) return '失败'
  return '进行中'
}

const getAllocationStatusColor = (status: string) => {
  const map: Record<string, string> = {
    pending: 'orange',
    executing: 'blue',
    completed: 'green',
    failed: 'red'
  }
  return map[status] || 'gray'
}

const getAllocationStatusText = (status: string) => {
  const map: Record<string, string> = {
    pending: '待执行',
    executing: '执行中',
    completed: '已完成',
    failed: '失败'
  }
  return map[status] || status
}

const handlePageChange = (page: number) => {
  logPagination.offset = (page - 1) * logPagination.limit
  fetchLogs()
}

const handleTabChange = (key: string) => {
  if (key === 'logs') {
    fetchLogs()
  } else if (key === 'allocations') {
    fetchAllocations()
  }
}

onMounted(() => {
  fetchNodes()
})
</script>

<template>
  <div class="cascade-management">
    <a-card title="级联节点管理" :bordered="false">
      <template #extra>
        <a-space>
          <a-button @click="fetchNodes" :loading="loading">
            <template #icon>
              <icon-refresh />
            </template>
            刷新
          </a-button>
          <a-button type="primary" @click="showCreateModal">+ 创建节点</a-button>
        </a-space>
      </template>

      <a-tabs v-model:active-key="activeTab" @change="handleTabChange">
        <a-tab-pane key="nodes" title="节点列表">
          <a-space direction="vertical" fill>
            <a-alert 
              v-if="error" 
              type="error" 
              show-icon 
              closable
              @close="error = ''"
            >
              {{ error }}
            </a-alert>
            
            <a-alert 
              type="info" 
              show-icon
            >
              级联系统支持父子节点架构。父节点负责统一管理数据，子节点从父节点同步并执行任务。
            </a-alert>
            
            <!-- 任务分发按钮 -->
            <a-space>
              <a-button type="primary" :loading="dispatching" @click="handleDispatchTask">
                <template #icon>
                  <icon-thunderbolt />
                </template>
                分发任务
              </a-button>
              <a-tooltip content="将消息任务分配给在线的子节点执行">
                <icon-question-circle style="color: #999;" />
              </a-tooltip>
            </a-space>

            <a-table
              :columns="columns"
              :data="nodeList"
              :loading="loading"
              row-key="id"
              :pagination="false"
              :scroll="{ x: 1400 }"
            >
              <template #node_type="{ record }">
                <a-tag :color="getNodeTypeColor(record.node_type)">
                  {{ getNodeTypeText(record.node_type) }}
                </a-tag>
              </template>

              <template #status="{ record }">
                <a-tag :color="getNodeStatusColor(record.status)">
                  {{ getNodeStatusText(record.status) }}
                </a-tag>
              </template>

              <template #last_heartbeat_at="{ record }">
                {{ formatDate(record.last_heartbeat_at) }}
              </template>

              <template #last_sync_at="{ record }">
                {{ formatDate(record.last_sync_at) }}
              </template>

              <template #action="{ record }">
                <a-space size="small">
                  <a-tooltip content="复制 API Key">
                    <a-button 
                      type="text" 
                      size="small"
                      @click="copyToClipboard(record.api_key || '')"
                      v-if="record.api_key"
                    >
                      <template #icon>
                        <icon-copy />
                      </template>
                    </a-button>
                  </a-tooltip>
                  <a-tooltip content="生成凭证">
                    <a-button 
                      type="text" 
                      size="small"
                      @click="handleGenerateCredentials(record)"
                      v-if="record.node_type === 1"
                    >
                      <template #icon>
                        <icon-link />
                      </template>
                    </a-button>
                  </a-tooltip>
                  <a-tooltip content="测试连接">
                    <a-button 
                      type="text" 
                      size="small"
                      @click="handleTestConnection(record)"
                      v-if="record.node_type === 1"
                    >
                      <template #icon>
                        <icon-refresh />
                      </template>
                    </a-button>
                  </a-tooltip>
                  <a-tooltip content="编辑">
                    <a-button 
                      type="text" 
                      size="small"
                      @click="editNode(record)"
                    >
                      <template #icon>
                        <icon-edit />
                      </template>
                    </a-button>
                  </a-tooltip>
                  <a-popconfirm 
                    content="确定要删除吗？"
                    ok-text="删除"
                    cancel-text="取消"
                    @ok="handleDelete(record)"
                  >
                    <a-button 
                      type="text" 
                      size="small"
                      status="danger"
                    >
                      <template #icon>
                        <icon-delete />
                      </template>
                    </a-button>
                  </a-popconfirm>
                </a-space>
              </template>
            </a-table>

            <a-empty v-if="!loading && nodeList.length === 0" description="暂无级联节点" />
          </a-space>
        </a-tab-pane>

        <a-tab-pane key="logs" title="同步日志">
          <a-table
            :columns="logColumns"
            :data="logList"
            :loading="logLoading"
            row-key="id"
            :pagination="{
              current: Math.floor(logPagination.offset / logPagination.limit) + 1,
              pageSize: logPagination.limit,
              total: totalLogs,
              showTotal: true,
              onChange: handlePageChange
            }"
          >
            <template #direction="{ record }">
              <a-tag :color="getDirectionColor(record.direction)">
                {{ getDirectionText(record.direction) }}
              </a-tag>
            </template>

            <template #status="{ record }">
              <a-tag :color="getLogStatusColor(record.status)">
                {{ getLogStatusText(record.status) }}
              </a-tag>
            </template>

            <template #started_at="{ record }">
              {{ formatDate(record.started_at) }}
            </template>

            <template #completed_at="{ record }">
              {{ formatDate(record.completed_at) }}
            </template>
          </a-table>

          <a-empty v-if="!logLoading && logList.length === 0" description="暂无同步日志" />
        </a-tab-pane>

        <a-tab-pane key="allocations" title="任务分配">
          <a-space direction="vertical" fill>
            <a-space>
              <a-button type="primary" :loading="dispatching" @click="handleDispatchTask">
                <template #icon>
                  <icon-thunderbolt />
                </template>
                分发任务
              </a-button>
              <a-button @click="fetchAllocations" :loading="allocationLoading">
                <template #icon>
                  <icon-refresh />
                </template>
                刷新
              </a-button>
            </a-space>

            <a-table
              :columns="allocationColumns"
              :data="allocationList"
              :loading="allocationLoading"
              row-key="allocation_id"
              :pagination="false"
            >
              <template #feed_count="{ record }">
                <a-tag color="blue">{{ record.feed_ids?.length || 0 }}</a-tag>
              </template>

              <template #allocation_status="{ record }">
                <a-tag :color="getAllocationStatusColor(record.status)">
                  {{ getAllocationStatusText(record.status) }}
                </a-tag>
              </template>

              <template #created_at="{ record }">
                {{ formatDate(record.created_at) }}
              </template>

              <template #updated_at="{ record }">
                {{ formatDate(record.updated_at) }}
              </template>
            </a-table>

            <a-empty v-if="!allocationLoading && allocationList.length === 0" description="暂无任务分配记录" />
          </a-space>
        </a-tab-pane>
      </a-tabs>
    </a-card>

    <!-- 创建/编辑节点模态框 -->
    <a-modal
      v-model:visible="visible"
      :title="modalTitle"
      width="600px"
      @ok="handleSubmit"
      @cancel="visible = false"
    >
      <a-form :model="form" layout="vertical">
        <a-form-item label="节点类型" field="node_type" required>
          <a-radio-group v-model="form.node_type">
            <a-radio :value="0">父节点</a-radio>
            <a-radio :value="1">子节点</a-radio>
          </a-radio-group>
          <div class="form-hint">父节点用于管理数据，子节点从父节点同步</div>
        </a-form-item>

        <a-form-item label="节点名称" field="name" required>
          <a-input 
            v-model="form.name" 
            placeholder="例如：节点1"
            @keyup.enter="handleSubmit"
          />
        </a-form-item>

        <a-form-item label="描述" field="description">
          <a-textarea 
            v-model="form.description" 
            placeholder="可选：节点说明、备注等"
            :rows="3"
          />
        </a-form-item>

        <a-form-item 
          label="API地址" 
          field="api_url"
          v-if="form.node_type === 1"
        >
          <a-input 
            v-model="form.api_url" 
            placeholder="例如：http://node-server:8001"
          />
          <div class="form-hint">子节点的服务地址，用于连接测试</div>
        </a-form-item>

        <a-alert 
          v-if="modalTitle === '创建节点'"
          type="info"
          show-icon
        >
          创建节点后，可以为子节点生成凭证用于连接父节点
        </a-alert>
      </a-form>
    </a-modal>

    <!-- 凭证显示模态框 -->
    <a-modal
      v-model:visible="showCredentialModal"
      title="节点凭证"
      width="600px"
      :footer="false"
      :closable="true"
    >
      <a-space direction="vertical" fill :size="16">
        <a-alert 
          type="warning" 
          show-icon
        >
          API Secret 只会显示一次，请立即复制并妥善保存！
        </a-alert>

        <a-descriptions :column="1" bordered>
          <a-descriptions-item label="API Key">
            <div style="display: flex; justify-content: space-between; align-items: center;">
              <code>{{ credentials.api_key }}</code>
              <a-button type="text" size="small" @click="copyToClipboard(credentials.api_key)">
                <template #icon>
                  <icon-copy />
                </template>
              </a-button>
            </div>
          </a-descriptions-item>
          <a-descriptions-item label="API Secret">
            <div style="display: flex; justify-content: space-between; align-items: center;">
              <code>{{ credentials.api_secret }}</code>
              <a-button type="text" size="small" @click="copyToClipboard(credentials.api_secret)">
                <template #icon>
                  <icon-copy />
                </template>
              </a-button>
            </div>
          </a-descriptions-item>
        </a-descriptions>

        <a-button type="primary" long @click="copyCredentials">
          复制全部
        </a-button>
      </a-space>
    </a-modal>
  </div>
</template>

<style scoped>
.cascade-management {
  padding: 20px;
}

.form-hint {
  font-size: 12px;
  color: #999;
  margin-top: 4px;
}

code {
  background: #f5f5f5;
  padding: 2px 6px;
  border-radius: 3px;
  font-family: 'Courier New', monospace;
  font-size: 13px;
}
</style>
