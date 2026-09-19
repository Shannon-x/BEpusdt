interface List {
  id: number;
  name: string;
  address: string;
  status: number;
  createTime: string;
  trade_type?: string;
  remark?: string;
  other_notify?: number;
  has_credentials?: boolean;
}

interface FormData {
  form: {
    name: string;
    address: string;
    trade_type: string;
  };
  search: boolean;
}

interface AddForm {
  name: string;
  address: string;
  trade_type: string;
  remark: string;
  other_notify: number;
  api_key?: string;
  api_secret?: string;
  passphrase?: string;
}

interface ModForm {
  id: number;
  name: string;
  status: number;
  address: string;
  trade_type: string;
  remark: string;
  other_notify: number;
  api_key?: string;
  api_secret?: string;
  passphrase?: string;
}

interface Pagination {
  showPageSize: boolean;
  showTotal: boolean;
  current: number;
  pageSize: number;
  total: number;
}

export type { List, FormData, Pagination, AddForm, ModForm };
