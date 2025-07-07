package com.kafkatps.kafkatps.enti;


import com.kafkatps.kafkatps.messages.UserContext;
import jakarta.persistence.Entity;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.util.Date;
import java.util.UUID;

@Table(name = "loans")
public class Loan implements Persistable<UUID> {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column("id")
  private UUID id;

  public Boolean getNewEntry() {
    return newEntry;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public boolean isMarkedToDelete() {
    return isMarkedToDelete;
  }

  public void setMarkedToDelete(boolean markedToDelete) {
    isMarkedToDelete = markedToDelete;
  }

  public String getServiceId() {
    return serviceId;
  }

  public UUID getVerticalId() {
    return verticalId;
  }

  public void setVerticalId(UUID verticalId) {
    this.verticalId = verticalId;
  }

  public UUID getTenantId() {
    return tenantId;
  }

  public void setTenantId(UUID tenantId) {
    this.tenantId = tenantId;
  }

  public UUID getLastUpdatedBy() {
    return lastUpdatedBy;
  }

  public void setLastUpdatedBy(UUID lastUpdatedBy) {
    this.lastUpdatedBy = lastUpdatedBy;
  }

  public Date getLastUpdatedDate() {
    return lastUpdatedDate;
  }

  public void setLastUpdatedDate(Date lastUpdatedDate) {
    this.lastUpdatedDate = lastUpdatedDate;
  }

  public String getLanguage() {
    return language;
  }

  public void setLanguage(String language) {
    this.language = language;
  }

  public Date getCreatedDate() {
    return createdDate;
  }

  public void setCreatedDate(Date createdDate) {
    this.createdDate = createdDate;
  }

  public UUID getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(UUID createdBy) {
    this.createdBy = createdBy;
  }

  public double getAmount() {
    return amount;
  }

  public UUID getMemberId() {
    return memberId;
  }

  @Column("member_id")
  private UUID memberId;

  @Column("amount")
  private double amount;

  @Column("created_by")
  private UUID createdBy;

  @Column("created_date")
  private Date createdDate;

  @Column("language")
  private String language;

  @Column("last_updated_date")
  private Date lastUpdatedDate;

  @Column("last_updated_by")
  private UUID lastUpdatedBy;

  @Column("tenant_id")
  private UUID tenantId;

  @Column("vertical_id")
  private UUID verticalId;

  @Column("service_id")
  private String serviceId;

  @Column("is_marked_to_delete")
  private boolean isMarkedToDelete;

  @Column("version")
  private int version;


  @org.springframework.data.annotation.Transient
  Boolean newEntry;


  @Override
  public UUID getId() {
    return this.id;
  }

  @Override
  public boolean isNew() {
    if (this.newEntry == null) {
      return false;
    } else {
      return this.newEntry;
    }
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public void setServiceId(String serviceId) {
    this.serviceId = serviceId;
  }

  public void assignEntityDefaults(UserContext userContext) {
    this.isMarkedToDelete = false;
    this.language = userContext.language();
    this.tenantId = userContext.tenantId();
    this.serviceId = userContext.serviceId();
    this.verticalId = userContext.verticalId();
    this.createdDate = this.lastUpdatedDate = new Date();
    this.createdBy = this.lastUpdatedBy = userContext.userId();
  }

  public void setNewEntry(Boolean newEntry) {
    this.newEntry = newEntry;
  }

  public void setAmount(double amount) {
    this.amount = amount;
  }

  public void setMemberId(UUID memberId) {
    this.memberId = memberId;
  }
}
