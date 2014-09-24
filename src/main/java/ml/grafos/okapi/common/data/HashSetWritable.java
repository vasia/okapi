package ml.grafos.okapi.common.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A HashSet that is also Writable. Subclass this abstract class and implement
 * the setClass() method to use with a specific type of elements.
 * 
 * @author dl
 *
 * @param <M>
 */
public abstract class HashSetWritable<M extends Writable> extends HashSet<M> 
  implements Writable, Configurable {

  /** Used for instantiation */
  private Class<M> refClass = null;

  /** Configuration */
  private Configuration conf;

  /**
   * Using the default constructor requires that the user implement
   * setClass(), guaranteed to be invoked prior to instantiation in
   * readFields()
   */
  public HashSetWritable() { }


  /**
   * This is a one-time operation to set the class type
   *
   * @param refClass internal type class
   */
  public void setClass(Class<M> refClass) {
    if (this.refClass != null) {
      throw new RuntimeException(
          "setClass: refClass is already set to " +
              this.refClass.getName());
    }
    this.refClass = refClass;
  }

  /**
   * Subclasses must set the class type appropriately and can use
   * setClass(Class<M> refClass) to do it.
   */
  public abstract void setClass();

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    if (this.refClass == null) {
      setClass();
    }

    clear();   // clear list before storing values
    int numValues = input.readInt();   
    for (int i = 0; i < numValues; i++) {
      M value = ReflectionUtils.newInstance(refClass, conf);
      value.readFields(input);
      add(value);             
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(size());
    for (M element : this) {
      element.write(output);
    }
  }
}
