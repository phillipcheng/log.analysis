package etl.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ConfigKey {
	Class<?> type() default String.class;
	/* Default value in string */
	String defaultValue() default "";
	/* Special format */
	String format() default "";
	/* Display title */
	String title() default "";
	/* Description */
	String description() default "";
}
