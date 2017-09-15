/**
 * 
 */
package io.microprofile.showcase.vote.health;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.health.Health;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;

/**
 * @author jagraj
 *
 */
@Health
@ApplicationScoped
public class SuccessfulHealthCheck implements HealthCheck {
	@Override
	public HealthCheckResponse call() {
		return HealthCheckResponse.named("Vote:successful-check").up().build();
	}
}
