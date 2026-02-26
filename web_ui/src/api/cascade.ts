import http from './http'

// 级联节点相关接口

export interface CascadeNode {
  id: string
  node_type: number  // 0=父节点, 1=子节点
  name: string
  description?: string
  api_url?: string
  api_key?: string
  parent_id?: string
  status: number  // 0=离线, 1=在线
  is_active: boolean
  last_sync_at?: string
  last_heartbeat_at?: string
  created_at: string
  updated_at: string
  sync_config?: any
}

export interface CreateNodeRequest {
  node_type: number
  name: string
  description?: string
  api_url?: string
}

export interface UpdateNodeRequest {
  name?: string
  description?: string
  api_url?: string
  is_active?: boolean
  sync_config?: any
}

export interface NodeCredentials {
  node_id: string
  api_key: string
  api_secret: string
}

export interface TestConnectionRequest {
  api_url: string
  api_key: string
  api_secret: string
}

export interface SyncLog {
  id: string
  node_id: string
  operation: string
  direction: string
  status: number
  data_count?: number
  error_message?: string
  extra_data?: any
  started_at: string
  completed_at?: string
}

export interface SyncLogsResponse {
  list: SyncLog[]
  page: {
    limit: number
    offset: number
  }
  total: number
}

// 创建级联节点
export const createNode = (data: CreateNodeRequest) => {
  return http.post<{ node_id: string, node_type: number, name: string, is_active: boolean, created_at: string }>('/wx/cascade/nodes', data)
}

// 获取节点列表
export const listNodes = (nodeType?: number) => {
  const params: any = {}
  if (nodeType !== undefined) {
    params.node_type = nodeType
  }
  return http.get<CascadeNode[]>('/wx/cascade/nodes', { params })
}

// 获取节点详情
export const getNode = (nodeId: string) => {
  return http.get<CascadeNode>(`/wx/cascade/nodes/${nodeId}`)
}

// 更新节点
export const updateNode = (nodeId: string, data: UpdateNodeRequest) => {
  return http.put(`/wx/cascade/nodes/${nodeId}`, data)
}

// 删除节点
export const deleteNode = (nodeId: string) => {
  return http.delete(`/wx/cascade/nodes/${nodeId}`)
}

// 生成节点凭证
export const generateNodeCredentials = (nodeId: string) => {
  return http.post<NodeCredentials>(`/wx/cascade/nodes/${nodeId}/credentials`)
}

// 测试节点连接
export const testNodeConnection = (nodeId: string, data?: TestConnectionRequest) => {
  return http.post(`/wx/cascade/nodes/${nodeId}/test-connection`, data)
}

// 获取同步日志
export const getSyncLogs = (params?: { node_id?: string; operation?: string; limit?: number; offset?: number }) => {
  return http.get<SyncLogsResponse>('/wx/cascade/sync-logs', { params })
}
