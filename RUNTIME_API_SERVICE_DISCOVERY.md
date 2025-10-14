# Runtime API Service Discovery実装

## 概要

AWS Service DiscoveryおよびConsul Service Discoveryを、HAProxyのRuntime API経由で動的にサーバーを追加/削除するように変更しました。これにより、Auto Scaling Groupのインスタンス増減時にHAProxyの**リロードなしで**設定を反映できます。

## 主な変更点

### 1. Service Discovery Instance (`discovery/service_discovery_instance.go`)

#### 新しい構造
```go
type ServiceDiscoveryInstance struct {
    client        configuration.Configuration
    haproxyClient client_native.HAProxyClient  // 追加: Runtime API用クライアント
    reloadAgent   haproxy.IReloadAgent
    services      map[string]*confService
    serverStates  map[string]map[string]bool  // 追加: サーバー状態の追跡
    transactionID string
    params        discoveryInstanceParams
}
```

#### 新しいメソッド

**`UpdateServices()`**
- Runtime APIを使用した動的更新を最初に試行
- 失敗した場合のみ従来の設定ファイル更新+リロードにフォールバック

**`updateServicesViaRuntime()`**
- Runtime APIを使用してサーバーを動的に追加/削除/更新
- サーバー状態を`serverStates`マップで追跡
- リロード不要

**`updateServicesViaConfigFile()`**
- 従来の設定ファイル更新方式（フォールバック用）
- トランザクションを使用して設定を更新
- リロードが必要

**ヘルパーメソッド:**
- `generateServerName()`: IPアドレスとポートから一意のサーバー名を生成
- `addServerViaRuntime()`: Runtime API経由でサーバーを追加
- `updateServerViaRuntime()`: Runtime API経由でサーバーのアドレスを更新
- `deleteServerViaRuntime()`: Runtime API経由でサーバーを削除
- `serializeRuntimeAddServer()`: サーバー追加コマンドのシリアライズ

### 2. Service Discovery Parameters (`discovery/service_discovery.go`)

```go
type ServiceDiscoveriesParams struct {
    Client        configuration.Configuration
    HAProxyClient client_native.HAProxyClient  // 追加
    ReloadAgent   haproxy.IReloadAgent
    Context       context.Context
}
```

### 3. AWS Service Discovery (`discovery/aws_service_discovery.go`)

- `awsServiceDiscovery`構造体に`haproxyClient`フィールドを追加
- `newAWSRegionInstance()`にHAProxyClient引数を追加

### 4. AWS Service Discovery Instance (`discovery/aws_service_discovery_instance.go`)

- `newAWSRegionInstance()`関数のシグネチャを更新してHAProxyClientを受け取る
- 初期化時にServiceDiscoveryInstanceにHAProxyClientを設定

### 5. Consul Service Discovery (`discovery/consul_service_discovery.go`)

- `consulServiceDiscovery`構造体に`haproxyClient`フィールドを追加
- Consulインスタンス初期化時にHAProxyClientを設定

### 6. Data Plane API Configuration (`configure_data_plane.go`)

```go
discovery := service_discovery.NewServiceDiscoveries(service_discovery.ServiceDiscoveriesParams{
    Client:        configurationClient,
    HAProxyClient: client,  // 追加
    ReloadAgent:   ra,
    Context:       ctx,
})
```

## 動作フロー

### インスタンス増加時

1. AWS/Consul APIがEC2インスタンスの追加を検出
2. `updateServicesViaRuntime()`が呼ばれる
3. 新しいサーバーを`runtime.AddServer()`で追加
4. `serverStates`マップを更新
5. **HAProxyはリロードせずに即座に新サーバーへのルーティング開始**

### インスタンス減少時

1. AWS/Consul APIがEC2インスタンスの削除を検出
2. `updateServicesViaRuntime()`が呼ばれる
3. 対象サーバーを`runtime.DisableServer()`でメンテナンスモードに
4. `runtime.DeleteServer()`でサーバーを削除
5. `serverStates`マップから削除
6. **HAProxyはリロードせずに即座にルーティングから除外**

### Runtime API失敗時

1. Runtime API経由での更新が失敗
2. 警告ログを出力
3. 自動的に`updateServicesViaConfigFile()`にフォールバック
4. 設定ファイルを更新してHAProxyをリロード

## 利点

### 1. ゼロダウンタイム
- HAProxyのリロードが不要なため、既存の接続に影響なし
- Auto Scalingによるインスタンス増減が即座に反映

### 2. 高速な反応
- 設定ファイルの書き込みやリロードプロセスが不要
- Runtime API経由の更新は数ミリ秒で完了

### 3. スケーラビリティ
- 頻繁なスケーリングイベントでもパフォーマンスに影響なし
- 設定ファイルの肥大化を回避

### 4. 信頼性
- Runtime API失敗時は従来の方式にフォールバック
- サーバー状態を追跡して一貫性を保証

## 制限事項

### 1. バックエンド作成
- 新しいバックエンド（サービス）の作成は設定ファイル更新が必要
- 初回のサービス検出時のみリロードが発生する可能性

### 2. HAProxy 2.6以上が必要
- Runtime APIの`add server`コマンドはHAProxy 2.6以降でサポート
- それ以前のバージョンでは自動的にフォールバックモードで動作

### 3. default_serverとの互換性
- `default_server`ディレクティブが定義されている場合、Runtime API経由のサーバー追加は動作しない
- その場合は自動的にフォールバックモードで動作

## テスト方法

### 手動テスト

1. AWS Auto Scaling Groupでインスタンスを追加
2. HAProxyのログを確認:
   ```
   Added server sd-10-0-1-100-8080 to backend aws-us-east-1-myapp-myservice-8080 via Runtime API
   ```
3. HAProxyがリロードされていないことを確認（プロセスIDが変わらない）

### Runtime APIの確認

```bash
# サーバーリストを確認
echo "show servers state" | socat stdio /var/run/haproxy.sock

# 特定のバックエンドのサーバーを確認
echo "show servers state aws-us-east-1-myapp-myservice-8080" | socat stdio /var/run/haproxy.sock
```

## ログ出力例

### 成功時
```
[INFO] Added server sd-10-0-1-100-8080 to backend aws-us-east-1-myapp-myservice-8080 via Runtime API
[INFO] Deleted server sd-10-0-1-99-8080 from backend aws-us-east-1-myapp-myservice-8080 via Runtime API
```

### フォールバック時
```
[WARN] Runtime API update failed: failed to add server: No such backend, falling back to config file update
```

## 今後の改善点

1. **サーバー属性の完全サポート**: 現在は基本的な属性のみ。weight, maxconn等の追加属性のサポート
2. **バックエンド動的作成**: Runtime API経由でのバックエンド作成（HAProxy 2.8+でサポート予定）
3. **状態の永続化**: サーバー状態を永続化して再起動後も維持
4. **メトリクス**: Runtime API経由での更新回数や成功率の監視

## 関連ファイル

- `discovery/service_discovery_instance.go` - メイン実装
- `discovery/aws_service_discovery.go` - AWS統合
- `discovery/aws_service_discovery_instance.go` - AWSインスタンス管理
- `discovery/consul_service_discovery.go` - Consul統合
- `discovery/service_discovery.go` - パラメータ定義
- `configure_data_plane.go` - 初期化設定
