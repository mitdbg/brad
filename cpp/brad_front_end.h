#pragma once

struct ServerInfo {
  std::string host;
  int port;
};

class BradFrontEnd {
 public:
  void AddServer(const std::string &host, int port);

  arrow::Status InitializeServer(const ServerInfo server);

  arrow::Status ExecuteQuery(const std::string &query);

 private:
  std::vector<ServerInfo> server_info_objects_;
};
