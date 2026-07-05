package org.plume.event;

import lombok.NonNull;

import java.io.Serializable;

public record Identity(@NonNull String identifier, String authentication) implements Serializable {}
