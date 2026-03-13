// BLite.Client.Java — BLiteAdminClient
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.admin;

import io.blite.client.BLiteError;
import io.blite.proto.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.stub.MetadataUtils;

import java.util.List;

/**
 * Administrative operations: user management and tenant provisioning.
 * All RPCs require Admin permission on {@code "*"}.
 */
public final class BLiteAdminClient {

    private static final io.grpc.Metadata.Key<String> API_KEY_HEADER =
            io.grpc.Metadata.Key.of("x-api-key", io.grpc.Metadata.ASCII_STRING_MARSHALLER);

    private final AdminServiceGrpc.AdminServiceBlockingStub stub;

    public BLiteAdminClient(ManagedChannel channel, String apiKey) {
        var headers = new io.grpc.Metadata();
        headers.put(API_KEY_HEADER, apiKey);
        stub = AdminServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    // ── User management ───────────────────────────────────────────────────────

    public CreateUserResult createUser(String username, List<UserPermission> permissions) {
        return createUser(username, null, null, permissions);
    }

    public CreateUserResult createUser(String username, String namespace, String databaseId,
                                       List<UserPermission> permissions) {
        var builder = CreateUserRequest.newBuilder().setUsername(username);
        if (namespace  != null) builder.setNamespace(namespace);
        if (databaseId != null) builder.setDatabaseId(databaseId);
        for (var p : permissions)
            builder.addPermissions(io.blite.proto.v1.UserPermission.newBuilder()
                    .setCollection(p.collection())
                    .setOps(p.ops())
                    .build());
        var resp = stub.createUser(builder.build());
        return new CreateUserResult(resp.getApiKey(), resp.getError());
    }

    public void revokeUser(String username) {
        var resp = stub.revokeUser(UsernameRequest.newBuilder().setUsername(username).build());
        BLiteError.check(resp.getError());
    }

    public CreateUserResult rotateKey(String username) {
        var resp = stub.rotateKey(UsernameRequest.newBuilder().setUsername(username).build());
        return new CreateUserResult(resp.getApiKey(), resp.getError());
    }

    public List<UserInfo> listUsers() {
        return stub.listUsers(Empty.newBuilder().build()).getUsersList();
    }

    public void updatePermissions(String username, List<UserPermission> permissions) {
        var builder = UpdatePermsRequest.newBuilder().setUsername(username);
        for (var p : permissions)
            builder.addPermissions(io.blite.proto.v1.UserPermission.newBuilder()
                    .setCollection(p.collection())
                    .setOps(p.ops())
                    .build());
        var resp = stub.updatePerms(builder.build());
        BLiteError.check(resp.getError());
    }

    // ── Tenant provisioning ───────────────────────────────────────────────────

    public void provisionTenant(String databaseId) {
        var resp = stub.provisionTenant(ProvisionTenantRequest.newBuilder()
                .setDatabaseId(databaseId).build());
        BLiteError.check(resp.getError());
    }

    public void deprovisionTenant(String databaseId, boolean deleteFiles) {
        var resp = stub.deprovisionTenant(DeprovisionTenantRequest.newBuilder()
                .setDatabaseId(databaseId)
                .setDeleteFiles(deleteFiles)
                .build());
        BLiteError.check(resp.getError());
    }

    public List<TenantInfo> listTenants() {
        return stub.listTenants(Empty.newBuilder().build()).getTenantsList();
    }
}
