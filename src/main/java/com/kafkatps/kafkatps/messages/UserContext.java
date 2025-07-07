package com.kafkatps.kafkatps.messages;

import java.util.List;
import java.util.UUID;

public record UserContext(UUID userId, UUID applicationId, UUID sessionId, UUID tenantId, UUID verticalId,
                          String serviceId, String email, String phoneNumber, String userName, String displayName,
                          String language, List<String> roles) {
}
